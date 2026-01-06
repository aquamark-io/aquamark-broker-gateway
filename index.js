const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const compression = require("compression");
const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const { PDFDocument } = require("pdf-lib");
const fetch = require("node-fetch");
const { createClient } = require("@supabase/supabase-js");
const sharp = require("sharp");
const crypto = require("crypto");
const AdmZip = require("adm-zip");
const rateLimit = require("express-rate-limit");
const winston = require("winston");

const app = express();
const PORT = process.env.PORT || 10000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const MAX_FILE_SIZE = 25 * 1024 * 1024;
const MAX_FUNDERS_PER_REQUEST = 15;
const MAX_FUNDER_NAME_LENGTH = 20;

// Cache limits (configurable for scaling)
const MAX_LOGO_CACHE_SIZE = 200;      // ~200-1000MB depending on logo sizes
const MAX_TEXT_CACHE_SIZE = 1000;     // ~5-10MB

// Simple caching with LRU support
const logoCache = new Map();
const textImageCache = new Map();

// ============================================
// STRUCTURED LOGGING (FIX #5)
// ============================================
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// ============================================
// RETRY UTILITY FOR NETWORK OPERATIONS
// ============================================
async function retryOperation(fn, maxRetries = 3, operationName = 'operation') {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const isLastAttempt = attempt === maxRetries;
      const isNetworkError = error.message.includes('fetch') || 
                            error.message.includes('timeout') ||
                            error.message.includes('ECONNREFUSED') ||
                            error.message.includes('ETIMEDOUT') ||
                            error.code === 'ENOTFOUND' ||
                            error.code === '503';
      
      if (isLastAttempt || !isNetworkError) {
        throw error;
      }
      
      const delayMs = 1000 * Math.pow(2, attempt - 1); // Exponential backoff: 1s, 2s, 4s
      logger.warn(`${operationName} failed, retrying`, { 
        attempt, 
        maxRetries, 
        delayMs,
        error: error.message 
      });
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
}

// ============================================
// RATE LIMITING (FIX #2)
// ============================================
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // 100 requests per 15 min per user
  message: 'Too many requests from this email. Please wait 15 minutes and try again.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    // Rate limit by user_email instead of IP
    return req.body.user_email || req.ip;
  },
  handler: (req, res) => {
    logger.warn('Rate limit exceeded', { 
      userEmail: req.body.user_email,
      ip: req.ip 
    });
    res.status(429).json({ 
      error: 'Too many requests. Please wait 15 minutes and try again.',
      retry_after: '15 minutes'
    });
  }
});

app.use(helmet());
app.use(compression());
app.use(express.json({ limit: '200mb' }));
app.use(cors());

// ============================================
// INPUT VALIDATION MIDDLEWARE (FIX #3)
// ============================================
function validateWatermarkRequest(req, res, next) {
  const { user_email, funder } = req.body;
  
  // Validate email format (simple check)
  if (!user_email || !user_email.includes('@')) {
    logger.warn('Invalid email format', { email: user_email });
    return res.status(400).json({ error: 'Valid email address required' });
  }
  
  // Validate funder exists and parse it
  let fundersList = [];
  if (typeof funder === 'string') {
    try {
      fundersList = JSON.parse(funder);
    } catch {
      fundersList = [funder];
    }
  } else if (Array.isArray(funder)) {
    fundersList = funder;
  } else if (req.body.funder) {
    if (Array.isArray(req.body.funder)) {
      fundersList = req.body.funder;
    } else {
      try {
        fundersList = JSON.parse(req.body.funder);
      } catch {
        fundersList = [req.body.funder];
      }
    }
  }
  
  if (fundersList.length === 0) {
    logger.warn('No funders provided', { userEmail: user_email });
    return res.status(400).json({ error: 'At least one funder required' });
  }
  
  if (fundersList.length > MAX_FUNDERS_PER_REQUEST) {
    logger.warn('Too many funders', { count: fundersList.length, userEmail: user_email });
    return res.status(400).json({ error: `Maximum ${MAX_FUNDERS_PER_REQUEST} funders per request` });
  }
  
  // Validate funder names (truncate if too long, reject special chars)
  const invalidChars = /[^a-zA-Z0-9\s\-]/;  // Only allow letters, numbers, spaces, hyphens
  
  try {
    req.validatedFunders = fundersList.map(f => {
      if (typeof f !== 'string') {
        logger.warn('Invalid funder type', { funder: f, userEmail: user_email });
        throw new Error('Funder names must be strings');
      }
      if (invalidChars.test(f)) {
        logger.warn('Funder name has invalid characters', { funder: f, userEmail: user_email });
        throw new Error(`Funder name contains invalid characters: "${f}". Only letters, numbers, spaces, and hyphens are allowed.`);
      }
      return f.substring(0, MAX_FUNDER_NAME_LENGTH);
    });
  } catch (error) {
    return res.status(400).json({ error: error.message });
  }
  
  next();
}

// ============================================
// CACHE MANAGEMENT UTILITIES
// ============================================

// LRU cache: removes oldest entry when limit is reached
function setWithLRULimit(cache, key, value, maxSize) {
  // If key already exists, delete it first (will re-add at end)
  if (cache.has(key)) {
    cache.delete(key);
  }
  
  // If at capacity, remove oldest (first) entry
  if (cache.size >= maxSize) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
  
  cache.set(key, value);
}

// Get from cache and refresh its position (move to end = most recently used)
function getWithLRURefresh(cache, key) {
  if (!cache.has(key)) {
    return null;
  }
  
  const value = cache.get(key);
  // Move to end by deleting and re-adding
  cache.delete(key);
  cache.set(key, value);
  return value;
}

// ============================================
// METADATA UTILITIES
// ============================================

/**
 * Adds custom metadata fields to PDF for automated processing
 * These fields help funders' OCR/automation systems identify:
 * 1. That the document is watermarked
 * 2. Which broker submitted it (for verification)
 * 3. Which funder this document is prepared for
 * 
 * IMPORTANT: This does NOT modify existing metadata (dates, creator, etc.)
 * which would trip fraud detection. It only adds NEW custom fields.
 */
function addAquamarkMetadata(pdfDoc, userEmail, funderName) {
  const { PDFName, PDFString } = require('pdf-lib');
  
  // Extract domain from email for broker identification
  const domain = userEmail.split('@')[1] || userEmail;
  const timestamp = new Date().toISOString();
  
  // Get the document's info dictionary
  const infoDict = pdfDoc.getInfoDict();
  
  // Add custom metadata fields using proper pdf-lib objects
  // These are standard PDF key-value pairs that won't interfere with forensics
  infoDict.set(PDFName.of('AquamarkProtected'), PDFString.of('true'));
  infoDict.set(PDFName.of('AquamarkBroker'), PDFString.of(domain));
  infoDict.set(PDFName.of('AquamarkFunder'), PDFString.of(funderName));
  infoDict.set(PDFName.of('AquamarkTimestamp'), PDFString.of(timestamp));
  
  // ALSO add to Keywords field (visible in most PDF viewers)
  // This makes it easily visible in document properties
  const existingKeywords = infoDict.get(PDFName.of('Keywords'));
  const keywordsText = existingKeywords ? existingKeywords.toString() : '';
  
  // Append Aquamark info to keywords with clear labels (Protected first, then Broker, then Funder)
  const aquamarkKeywords = `AquamarkProtected: true, AquamarkBroker: ${domain}, AquamarkFunder: ${funderName}`;
  const newKeywords = keywordsText 
    ? `${keywordsText}, ${aquamarkKeywords}`
    : aquamarkKeywords;
  
  infoDict.set(PDFName.of('Keywords'), PDFString.of(newKeywords));
  
  // Note: We're NOT calling pdfDoc.setTitle(), setAuthor(), etc.
  // which would modify the existing metadata that fraud detection checks
}

// ============================================
// CORE FUNCTIONS
// ============================================

async function getCachedLogo(userEmail) {
  // Check cache with LRU refresh
  const cached = getWithLRURefresh(logoCache, userEmail);
  if (cached) {
    return cached;
  }
  
  // Wrap in retry logic for network resilience
  return await retryOperation(async () => {
    const { data: logoList } = await supabase.storage.from("logos").list(userEmail);
    if (!logoList || logoList.length === 0) throw new Error("No logo found");

    const actualLogos = logoList.filter(file => 
      !file.name.includes('emptyFolderPlaceholder') && 
      !file.name.includes('.emptyFolderPlaceholder') &&
      (file.name.includes('logo-') || file.name.endsWith('.png') || file.name.endsWith('.jpg'))
    );
    
    if (actualLogos.length === 0) throw new Error("No logo found");

    const logo = actualLogos[0];
    const logoPath = `${userEmail}/${logo.name}`;
    const { data: logoUrlData } = supabase.storage.from("logos").getPublicUrl(logoPath);
    const logoRes = await fetch(logoUrlData.publicUrl);
    const logoBytes = Buffer.from(await logoRes.arrayBuffer());
    
    setWithLRULimit(logoCache, userEmail, logoBytes, MAX_LOGO_CACHE_SIZE);
    return logoBytes;
  }, 3, 'Logo fetch');
}

async function checkUserAuthorization(userEmail) {
  const { data: user, error } = await supabase
    .from("users")
    .select("*")
    .eq("email", userEmail)
    .single();
  
  if (error || !user) throw new Error("User not found");
  
  if (["Leak Detection"].includes(user.plan)) {
    return { authorized: true, user };
  }
  return { authorized: false, reason: "Requires the Leak Detection plan" };
}

async function logBrokerUsage(userEmail, fileCount, pageCount) {
  try {
    const now = new Date();
    const { error } = await supabase.rpc('increment_broker_usage', {
      p_user_email: userEmail,
      p_month: now.getMonth() + 1,
      p_year: now.getFullYear(),
      p_file_count: fileCount,
      p_page_count: pageCount
    });

    if (error) logger.error('Usage log error', { error: error.message, userEmail });
  } catch (err) {
    logger.error('Usage logging failed (non-fatal)', { error: err.message, userEmail });
  }
}

function isPdfEncrypted(buffer) {
  const content = buffer.toString('latin1');
  return content.includes('/Encrypt') && !content.includes('/Encrypt null');
}

async function optimizePdfWithQpdf(pdfBuffer) {
  const tempId = crypto.randomUUID();
  const inPath = path.join(__dirname, `temp-${tempId}-in.pdf`);
  const outPath = path.join(__dirname, `temp-${tempId}-out.pdf`);
  
  try {
    fs.writeFileSync(inPath, pdfBuffer);
    
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Optimization timeout")), 15000);
      
      exec(`qpdf --compress-streams=y --object-streams=generate "${inPath}" "${outPath}"`, 
        (error, stdout, stderr) => {
          clearTimeout(timeout);
          if (error) {
            reject(new Error(`Optimization failed: ${stderr || error.message}`));
          } else {
            resolve();
          }
        });
    });
    
    const optimizedBuffer = fs.readFileSync(outPath);
    const originalSize = pdfBuffer.length;
    const optimizedSize = optimizedBuffer.length;
    
    return optimizedSize < originalSize ? optimizedBuffer : pdfBuffer;
  } catch (error) {
    return pdfBuffer;
  } finally {
    try {
      if (fs.existsSync(inPath)) fs.unlinkSync(inPath);
      if (fs.existsSync(outPath)) fs.unlinkSync(outPath);
    } catch (cleanupError) {
      logger.error('Temp file cleanup failed', { error: cleanupError.message });
    }
  }
}

async function renderTextAsImage(text, fontSize, color, opacity) {
  const cacheKey = `${text}:${fontSize}`;
  
  const cached = getWithLRURefresh(textImageCache, cacheKey);
  if (cached) {
    return cached;
  }
  
  const truncatedText = text.substring(0, MAX_FUNDER_NAME_LENGTH);
  
  // Render at high DPI for sharpness
  const renderDpi = 300;
  const renderScale = renderDpi / 72;
  const scaledFontSize = fontSize * renderScale;
  
  // Calculate render dimensions (high resolution)
  const renderWidth = MAX_FUNDER_NAME_LENGTH * scaledFontSize * 0.55;
  const renderHeight = scaledFontSize * 1.2;
  
  const svg = `
    <svg width="${renderWidth}" height="${renderHeight}" xmlns="http://www.w3.org/2000/svg">
      <text 
        x="${renderWidth / 2}" 
        y="${scaledFontSize}" 
        font-family="Helvetica, Arial, sans-serif" 
        font-weight="bold"
        font-size="${scaledFontSize}" 
        fill="rgb(${Math.round(color.red * 255)}, ${Math.round(color.green * 255)}, ${Math.round(color.blue * 255)})"
        opacity="${opacity}"
        text-anchor="middle">
        ${truncatedText}
      </text>
    </svg>
  `;
  
  const textImageBuffer = await sharp(Buffer.from(svg))
    .png({ compressionLevel: 9, palette: true })
    .toBuffer();
  
  // Return DISPLAY dimensions (72 DPI) so text appears correct size in PDF
  const displayWidth = MAX_FUNDER_NAME_LENGTH * fontSize * 0.55;
  const displayHeight = fontSize * 1.2;
  
  const result = { 
    buffer: textImageBuffer, 
    width: displayWidth, 
    height: displayHeight
  };
  
  setWithLRULimit(textImageCache, cacheKey, result, MAX_TEXT_CACHE_SIZE);
  return result;
}

const WATERMARK_POSITIONS = [
  { x: -37, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 92.6, y: 167.4, containerWidth: 200, containerHeight: 130 },  // Row 2
  { x: 337.4, y: 167.4, containerWidth: 200, containerHeight: 130 },  // Row 2
  { x: -37, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 92.6, y: 484.2, containerWidth: 200, containerHeight: 130 },  // Row 4
  { x: 337.4, y: 484.2, containerWidth: 200, containerHeight: 130 },  // Row 4
  { x: -37, y: 642.6, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 642.6, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 642.6, containerWidth: 200, containerHeight: 130 }
];

function getCenteredWatermarkWithText(container, logoWidth, logoHeight, textWidth, textHeight) {
  const logoX = container.x + (container.containerWidth - logoWidth) / 2;
  const logoY = container.y + (container.containerHeight - logoHeight - textHeight - 25) / 2 + textHeight + 25;
  const gap = 25; // Increased from 8 to 25 to account for 45-degree rotation
  const textX = container.x + (container.containerWidth - textWidth) / 2 + 20; // Shift right by 20px for better centering
  const textY = logoY - textHeight - gap;
  
  return { logoX, logoY, textX, textY };
}

// OPTIMIZED: Now accepts already-parsed pdfDoc instead of raw bytes
async function createWatermarkedPdfForFunder(pdfDoc, funderName, logoBytes, pageWidth, pageHeight) {
  // Create a new PDF document (don't let pdf-lib add its own metadata)
  const funderPdfDoc = await PDFDocument.create({ updateMetadata: false });
  
  // PRESERVE ORIGINAL METADATA - copy from source PDF before adding pages
  const { PDFName, PDFString, PDFHexString, PDFArray, PDFDict } = require('pdf-lib');
  
  // Get source and target info dictionaries
  const sourceInfoDict = pdfDoc.getInfoDict();
  const targetInfoDict = funderPdfDoc.getInfoDict();
  
  // Copy ALL metadata fields from source to target in one pass
  // This ensures we don't miss ANY field that fraud detection might check
  sourceInfoDict.entries().forEach(([key, value]) => {
    // Copy everything except Aquamark fields (in case we're reprocessing)
    const keyName = key.toString();
    if (!keyName.includes('Aquamark')) {
      targetInfoDict.set(key, value);
    }
  });
  
  // Now copy the pages
  const copiedPages = await funderPdfDoc.copyPages(pdfDoc, pdfDoc.getPageIndices());
  
  // Add all copied pages to the new document
  for (const page of copiedPages) {
    funderPdfDoc.addPage(page);
  }

  // Create a SEPARATE watermark PDF (tamper-resistant approach)
  const watermarkDoc = await PDFDocument.create();
  const watermarkLogo = await watermarkDoc.embedPng(logoBytes);

  const logoWidth = 75;
  const logoHeight = (watermarkLogo.height / watermarkLogo.width) * logoWidth;

  const { buffer: textImageBuffer, width: textWidth, height: textHeight } = await renderTextAsImage(
    funderName, 
    8, 
    { red: 0.8, green: 0.1, blue: 0.1 }, 
    0.5
  );

  const watermarkText = await watermarkDoc.embedPng(textImageBuffer);

  // Create a single watermark page with all marks positioned
  const watermarkPage = watermarkDoc.addPage([pageWidth, pageHeight]);

  // Add all logo + text watermarks to the watermark page
  for (const container of WATERMARK_POSITIONS) {
    const { logoX, logoY, textX, textY } = getCenteredWatermarkWithText(
      container, logoWidth, logoHeight, textWidth, textHeight
    );
    
    watermarkPage.drawImage(watermarkText, {
      x: textX,
      y: textY,
      width: textWidth,
      height: textHeight,
      opacity: 0.30,
      rotate: { angle: Math.PI / 4, type: 'radians' } // 45 degrees
    });

    watermarkPage.drawImage(watermarkLogo, {
      x: logoX,
      y: logoY,
      width: logoWidth,
      height: logoHeight,
      opacity: 0.30,
      rotate: { angle: Math.PI / 4, type: 'radians' } // 45 degrees
    });
  }

  // Save the watermark PDF and embed it as a page object
  const watermarkPdfBytes = await watermarkDoc.save();
  const watermarkEmbed = await PDFDocument.load(watermarkPdfBytes);
  const [embeddedPage] = await funderPdfDoc.embedPages([watermarkEmbed.getPages()[0]]);

  // Draw the embedded watermark page onto ALL pages (efficient + tamper-resistant)
  const pages = funderPdfDoc.getPages();
  pages.forEach((page) => {
    const { width, height } = page.getSize();
    page.drawPage(embeddedPage, { x: 0, y: 0, width, height });
  });

  const pdfBytes = await funderPdfDoc.save({
    useObjectStreams: false,
    addDefaultPage: false,
    objectsPerTick: 50,
    updateMetadata: false
  });
  return pdfBytes;
}

// ============================================
// PDF VALIDATION (FIX #3 - IMPROVED)
// ============================================
async function validateAndLoadPdf(buffer, fileName) {
  try {
    // Basic check: does it start with %PDF?
    const header = buffer.slice(0, 5).toString('latin1');
    if (!header.startsWith('%PDF')) {
      throw new Error('File does not appear to be a PDF');
    }
    
    // Try to load with pdf-lib (this will catch most corrupted PDFs)
    const pdfDoc = await PDFDocument.load(buffer, { 
      ignoreEncryption: true,
      updateMetadata: false 
    });
    
    return pdfDoc;
  } catch (error) {
    // Log the error but throw a user-friendly message
    logger.error('PDF validation failed', { 
      fileName, 
      error: error.message,
      fileSize: buffer.length 
    });
    throw new Error(`Unable to process ${fileName}. File may be corrupted or password-protected.`);
  }
}

async function createJob(jobId, userEmail) {
  const { error } = await supabase
    .from('broker_jobs')
    .insert({
      id: jobId,
      user_email: userEmail,
      status: 'processing',
      created_at: new Date().toISOString()
    });
  
  if (error) throw new Error('Failed to create job: ' + error.message);
}

async function updateJobProgress(jobId, progress) {
  await supabase
    .from('broker_jobs')
    .update({ progress })
    .eq('id', jobId);
}

async function completeJob(jobId, downloadUrl, storagePath) {
  await supabase
    .from('broker_jobs')
    .update({
      status: 'complete',
      download_url: downloadUrl,
      storage_path: storagePath,
      completed_at: new Date().toISOString()
    })
    .eq('id', jobId);
}

async function failJob(jobId, errorMessage) {
  await supabase
    .from('broker_jobs')
    .update({
      status: 'error',
      error_message: errorMessage,
      completed_at: new Date().toISOString()
    })
    .eq('id', jobId);
}

async function deleteFromStorage(storagePath) {
  const { error } = await supabase.storage
    .from('broker-job-results')
    .remove([storagePath]);
  
  if (error) throw new Error('Delete failed: ' + error.message);
}

async function processJobInBackground(jobId, userEmail, filesList, fundersList) {
  try {
    const startTime = Date.now();
    logger.info('Job started', { jobId, userEmail, fileCount: filesList.length, funderCount: fundersList.length });
    
    const logoBytes = await getCachedLogo(userEmail);
    const zip = new AdmZip();
    
    let totalSuccess = 0;
    let totalFailures = 0;
    let totalPageCount = 0;
    
    for (let fileIndex = 0; fileIndex < filesList.length; fileIndex++) {
      const file = filesList[fileIndex];
      
      // Update progress
      await updateJobProgress(jobId, `Processing ${fileIndex + 1} of ${filesList.length} files`);
      
      try {
        // Fetch PDF
        let pdfBytes;
        if (file.url) {
          const pdfRes = await fetch(file.url);
          if (!pdfRes.ok) throw new Error(`Failed to fetch ${file.url}`);
          pdfBytes = Buffer.from(await pdfRes.arrayBuffer());
        } else {
          pdfBytes = file.data;
        }
        
        if (pdfBytes.length > MAX_FILE_SIZE) {
          throw new Error(`File exceeds 25MB limit`);
        }
        
        // Load PDF
        let pdfDoc;
        try {
          pdfDoc = await PDFDocument.load(pdfBytes, { 
            ignoreEncryption: false,
            updateMetadata: false
          });
        } catch (loadError) {
          if (isPdfEncrypted(pdfBytes)) {
            const tempId = crypto.randomUUID();
            const inPath = path.join(__dirname, `temp-${tempId}.pdf`);
            const outPath = path.join(__dirname, `temp-${tempId}-dec.pdf`);
            
            try {
              fs.writeFileSync(inPath, pdfBytes);
              
              await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error("Decryption timeout")), 10000);
                
                exec(`qpdf --decrypt "${inPath}" "${outPath}"`, (error, stdout, stderr) => {
                  clearTimeout(timeout);
                  if (error) {
                    reject(new Error(`Decryption failed: ${stderr || error.message}`));
                  } else {
                    resolve();
                  }
                });
              });
              
              pdfBytes = fs.readFileSync(outPath);
              
              pdfDoc = await PDFDocument.load(pdfBytes, { 
                ignoreEncryption: true,
                updateMetadata: false
              });
            } finally {
              try {
                if (fs.existsSync(inPath)) fs.unlinkSync(inPath);
                if (fs.existsSync(outPath)) fs.unlinkSync(outPath);
              } catch (cleanupError) {
                logger.error('Decryption temp file cleanup failed', { error: cleanupError.message });
              }
            }
          } else {
            pdfDoc = await PDFDocument.load(pdfBytes, { 
              ignoreEncryption: true,
              updateMetadata: false,
              throwOnInvalidObject: false
            });
          }
        }
        
        const pages = pdfDoc.getPages();
        if (!pages || pages.length === 0) {
          throw new Error("PDF has no pages");
        }
        
        // Optimize PDF
        const optimizedPdfBytes = await pdfDoc.save({ useObjectStreams: true, updateMetadata: false });
        const finalOptimizedBytes = await optimizePdfWithQpdf(optimizedPdfBytes);
        
        const optimizedPdfDoc = await PDFDocument.load(finalOptimizedBytes, {
          updateMetadata: false
        });
        
        const { width, height } = optimizedPdfDoc.getPages()[0].getSize();
        const pageCount = optimizedPdfDoc.getPageCount();
        totalPageCount += pageCount;
        
        const baseName = file.name.replace(/\.pdf$/i, "");
        
        // Process funders in batches
        const BATCH_SIZE = 4;
        const allResults = [];
        
        for (let i = 0; i < fundersList.length; i += BATCH_SIZE) {
          const batch = fundersList.slice(i, i + BATCH_SIZE);
          
          const batchResults = await Promise.allSettled(
            batch.map(async (funderName) => {
              const pdfBuffer = await createWatermarkedPdfForFunder(
                optimizedPdfDoc, funderName, logoBytes, width, height
              );
              
              const finalPdfDoc = await PDFDocument.load(pdfBuffer, {
                updateMetadata: false
              });
              addAquamarkMetadata(finalPdfDoc, userEmail, funderName);
              const finalPdfBuffer = await finalPdfDoc.save({ useObjectStreams: true, updateMetadata: false });
              
              const funderSlug = funderName.substring(0, MAX_FUNDER_NAME_LENGTH)
                .replace(/\s+/g, '-').toLowerCase();
              const pdfFilename = `${baseName}-${funderSlug}.pdf`;
              
              return { filename: pdfFilename, buffer: finalPdfBuffer, funderName };
            })
          );
          
          allResults.push(...batchResults);
        }
        
        // Add successful PDFs to zip
        let fileSuccessCount = 0;
        for (const result of allResults) {
          if (result.status === 'fulfilled') {
            const { filename, buffer } = result.value;
            zip.addFile(filename, buffer);
            totalSuccess++;
            fileSuccessCount++;
          } else {
            logger.error('Failed to process funder', { 
              fileName: file.name, 
              error: result.reason 
            });
            totalFailures++;
          }
        }
        
        if (fileSuccessCount > 0 && fileSuccessCount < fundersList.length) {
          logger.info('Partial file success', { 
            fileName: file.name, 
            successCount: fileSuccessCount, 
            totalFunders: fundersList.length 
          });
        }
        
      } catch (pdfError) {
        logger.error('Failed to process PDF', { 
          fileName: file.name, 
          error: pdfError.message 
        });
        totalFailures += fundersList.length;
        continue;
      }
    }
    
    if (totalSuccess === 0) {
      throw new Error("All files/funders failed to process");
    }
    
    // Upload ZIP to storage
    const zipBuffer = zip.toBuffer();
    const zipFileName = `${jobId}.zip`;
    
    // Wrap upload in retry logic for network resilience
    await retryOperation(async () => {
      const { error: uploadError } = await supabase.storage
        .from('broker-job-results')
        .upload(zipFileName, zipBuffer, {
          contentType: 'application/zip',
          upsert: true
        });
      
      if (uploadError) throw new Error('Upload failed: ' + uploadError.message);
    }, 3, 'Storage upload');
    
    const { data: urlData } = supabase.storage
      .from('broker-job-results')
      .getPublicUrl(zipFileName);
    
    await completeJob(jobId, urlData.publicUrl, zipFileName);
    
    // Log usage
    await logBrokerUsage(userEmail, totalSuccess, totalPageCount * fundersList.length);
    
    const elapsed = Date.now() - startTime;
    const failureMsg = totalFailures > 0 ? ` (${totalFailures} failed)` : '';
    logger.info('Job completed', { 
      jobId, 
      userEmail, 
      successCount: totalSuccess, 
      failureCount: totalFailures,
      elapsedMs: elapsed 
    });
    
  } catch (err) {
    logger.error('Job failed', { jobId, userEmail, error: err.message, stack: err.stack });
    await failJob(jobId, err.message);
  }
}

// ============================================
// API ENDPOINTS
// ============================================

app.post("/watermark-broker-funder", apiLimiter, validateWatermarkRequest, async (req, res) => {
  try {
    const userEmail = req.body.user_email;
    const fundersList = req.validatedFunders; // From validation middleware
    
    // Check API key
    const authHeader = req.headers["authorization"];
    if (!authHeader || !authHeader.startsWith("Bearer ")) {
      return res.status(401).send("Missing authorization header");
    }
    
    const token = authHeader.split(" ")[1];
    if (token !== process.env.AQUAMARK_API_KEY) {
      return res.status(401).send("Invalid API key");
    }
    
    // Check user authorization
    const { authorized, reason } = await checkUserAuthorization(userEmail);
    if (!authorized) {
      return res.status(402).send(reason);
    }
    
    // NEW: Accept 'files' array with base64 or URL support (matching Funder API)
    const { files: filesArray } = req.body;
    
    // Validate files array exists
    if (!filesArray || !Array.isArray(filesArray) || filesArray.length === 0) {
      return res.status(400).json({ 
        error: "Missing 'files' array. Provide array of file objects with 'name' and either 'data' (base64) or 'url'" 
      });
    }
    
    const files = [];
    
    // Process each file object
    for (let i = 0; i < filesArray.length; i++) {
      const fileObj = filesArray[i];
      
      // Validate file object has required properties
      if (!fileObj.name) {
        return res.status(400).json({ 
          error: `File at index ${i} missing 'name' property` 
        });
      }
      
      // Check if it has 'data' (base64) or 'url'
      if (fileObj.data && fileObj.url) {
        return res.status(400).json({ 
          error: `File '${fileObj.name}' has both 'data' and 'url'. Provide only one.` 
        });
      }
      
      if (!fileObj.data && !fileObj.url) {
        return res.status(400).json({ 
          error: `File '${fileObj.name}' missing 'data' or 'url' property` 
        });
      }
      
      let buffer;
      
      // Handle base64 data
      if (fileObj.data) {
        try {
          const base64Data = fileObj.data.replace(/^data:application\/pdf;base64,/, '');
          buffer = Buffer.from(base64Data, 'base64');
          
          // Validate it's a PDF
          if (!buffer.toString('utf8', 0, 4).includes('PDF')) {
            return res.status(400).json({ 
              error: `File '${fileObj.name}' is not a valid PDF` 
            });
          }
        } catch (error) {
          return res.status(400).json({ 
            error: `File '${fileObj.name}' has invalid base64 encoding` 
          });
        }
      }
      // Handle URL
      else if (fileObj.url) {
        try {
          const response = await fetch(fileObj.url);
          if (!response.ok) {
            return res.status(400).json({ 
              error: `Failed to download file '${fileObj.name}' from URL` 
            });
          }
          buffer = Buffer.from(await response.arrayBuffer());
        } catch (error) {
          return res.status(400).json({ 
            error: `Failed to download file '${fileObj.name}': ${error.message}` 
          });
        }
      }
      
      // Validate file size
      if (buffer.length > MAX_FILE_SIZE) {
        return res.status(400).json({ 
          error: `File '${fileObj.name}' exceeds 25MB limit (${Math.round(buffer.length / 1024 / 1024)}MB)` 
        });
      }
      
      // Validate filename has .pdf extension
      if (!fileObj.name.toLowerCase().endsWith('.pdf')) {
        return res.status(400).json({ 
          error: `File '${fileObj.name}' must have .pdf extension` 
        });
      }
      
      files.push({
        name: fileObj.name,
        data: buffer
      });
    }
    
    // Create job
    const jobId = crypto.randomUUID();
    await createJob(jobId, userEmail);
    
    logger.info('Job created', { 
      jobId, 
      userEmail,
      fileCount: files.length
    });
    
    // Start background processing (don't await)
    processJobInBackground(jobId, userEmail, files, fundersList).catch(err => {
      logger.error('Background job failed', { jobId, error: err.message });
    });
    
    // Return job ID immediately
    res.json({
      job_id: jobId,
      status: 'processing',
      file_count: files.length,
      message: 'Job created successfully. Poll /job-status/{job_id} for updates.'
    });
    
  } catch (err) {
    logger.error('Error creating job', { error: err.message, stack: err.stack });
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get("/job-status/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    
    const { data: job, error } = await supabase
      .from('broker_jobs')
      .select('*')
      .eq('id', jobId)
      .single();
    
    if (error || !job) {
      return res.status(404).json({ error: 'Job not found' });
    }
    
    // Check for timeout: if job is still processing after 10 minutes, mark as error
    if (job.status === 'processing') {
      const createdAt = new Date(job.created_at);
      const now = new Date();
      const minutesElapsed = (now - createdAt) / 1000 / 60;
      
      if (minutesElapsed > 10) {
        // Update job to error status
        await supabase
          .from('broker_jobs')
          .update({
            status: 'error',
            error_message: 'Job timed out after 10 minutes',
            completed_at: new Date().toISOString()
          })
          .eq('id', jobId);
        
        logger.warn('Job timed out', { jobId });
        
        return res.json({
          job_id: job.id,
          status: 'error',
          error_message: 'Job timed out after 10 minutes',
          created_at: job.created_at,
          completed_at: new Date().toISOString()
        });
      }
    }
    
    // If complete and has download URL
    if (job.status === 'complete' && job.download_url) {
      // Check if file was already downloaded (by checking if storage_path still exists)
      const { data: files } = await supabase.storage
        .from('broker-job-results')
        .list('', { search: job.storage_path });
      
      // If file doesn't exist, it was already downloaded and deleted
      const fileExists = files && files.length > 0;
      
      res.json({
        job_id: job.id,
        status: job.status,
        download_url: fileExists ? job.download_url : null,
        message: fileExists ? 'Ready for download. Files expire after 1 hour.' : 'File already downloaded and removed',
        created_at: job.created_at,
        completed_at: job.completed_at
      });
      
      // FIX #1: REMOVED setTimeout - files now expire after 1 hour naturally
      // Users are informed to download promptly
      
    } else if (job.status === 'error') {
      res.json({
        job_id: job.id,
        status: job.status,
        error_message: job.error_message,
        created_at: job.created_at,
        completed_at: job.completed_at
      });
    } else {
      res.json({
        job_id: job.id,
        status: job.status,
        progress: job.progress,
        created_at: job.created_at
      });
    }
  } catch (err) {
    logger.error('Error fetching job status', { error: err.message });
    res.status(500).json({ error: 'Failed to fetch job status' });
  }
});

// Endpoint to confirm download and delete file
app.post("/job-complete/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    
    const { data: job, error } = await supabase
      .from('broker_jobs')
      .select('storage_path')
      .eq('id', jobId)
      .single();
    
    if (error || !job) {
      return res.status(404).json({ error: 'Job not found' });
    }
    
    if (job.storage_path) {
      await deleteFromStorage(job.storage_path);
      logger.info('File deleted after download', { jobId, storagePath: job.storage_path });
    }
    
    res.json({ message: 'File deleted successfully' });
  } catch (err) {
    logger.error('Error deleting file', { error: err.message });
    res.status(500).json({ error: 'Failed to delete file' });
  }
});

app.get("/health", (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    status: "healthy",
    memory: Math.round(mem.heapUsed / 1024 / 1024) + "MB",
    caches: {
      logos: logoCache.size,
      textImages: textImageCache.size
    },
    uptime: Math.round(process.uptime()) + "s"
  });
});

app.post("/clear-cache", (req, res) => {
  const authHeader = req.headers["authorization"];
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).send("Missing authorization");
  }

  const token = authHeader.split(" ")[1];
  if (token !== process.env.AQUAMARK_API_KEY) {
    return res.status(401).send("Invalid API key");
  }

  const logoSize = logoCache.size;
  const textSize = textImageCache.size;
  
  logoCache.clear();
  textImageCache.clear();
  
  logger.info('Cache cleared', { logos: logoSize, textImages: textSize });
  
  res.json({ 
    cleared: { 
      logos: logoSize, 
      textImages: textSize
    } 
  });
});

app.post("/clear-user-cache", (req, res) => {
  const authHeader = req.headers["authorization"];
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).send("Missing authorization");
  }

  const token = authHeader.split(" ")[1];
  if (token !== process.env.AQUAMARK_API_KEY) {
    return res.status(401).send("Invalid API key");
  }

  const userEmail = req.body.user_email;
  if (!userEmail) {
    return res.status(400).send("Missing user_email");
  }

  let logoCleared = false;
  if (logoCache.has(userEmail)) {
    logoCache.delete(userEmail);
    logoCleared = true;
  }
  
  logger.info('User cache cleared', { userEmail, logoCleared });
  
  res.json({ 
    user: userEmail,
    cleared: { 
      logo: logoCleared,
      message: logoCleared ? "Logo cache cleared successfully" : "No cached logo found for user"
    } 
  });
});

// ============================================
// GLOBAL ERROR HANDLER (FIX #4)
// ============================================
app.use((err, req, res, next) => {
  logger.error('Unhandled error', { 
    error: err.message, 
    stack: err.stack,
    path: req.path,
    method: req.method
  });
  
  // Don't leak error details to client in production
  const isDevelopment = process.env.NODE_ENV === 'development';
  
  res.status(err.status || 500).json({
    error: isDevelopment ? err.message : 'Internal server error',
    message: 'An unexpected error occurred. Please try again.'
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// ============================================
// START SERVER
// ============================================
app.listen(PORT, () => {
  logger.info('Server started', { 
    port: PORT,
    cacheLimit: { logos: MAX_LOGO_CACHE_SIZE, textImages: MAX_TEXT_CACHE_SIZE }
  });
  console.log(`🚀 Aquamark Broker API (Async) on port ${PORT}`);
  console.log(`📦 Cache limits: ${MAX_LOGO_CACHE_SIZE} logos, ${MAX_TEXT_CACHE_SIZE} text images`);
});
