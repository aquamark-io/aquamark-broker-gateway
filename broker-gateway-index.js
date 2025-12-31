const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const { PDFDocument } = require("pdf-lib");
const fetch = require("node-fetch");
const { createClient } = require("@supabase/supabase-js");
const sharp = require("sharp");
const crypto = require("crypto");
const winston = require("winston");

const app = express();
const PORT = process.env.PORT || 10000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const POSTMARK_API_KEY = process.env.POSTMARK_API_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const MAX_FILE_SIZE = 25 * 1024 * 1024;
const MAX_FUNDER_NAME_LENGTH = 20;

// Cache limits
const MAX_LOGO_CACHE_SIZE = 200;
const MAX_TEXT_CACHE_SIZE = 1000;

// Caching with LRU support
const logoCache = new Map();
const textImageCache = new Map();

// ============================================
// STRUCTURED LOGGING
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
// CACHE MANAGEMENT UTILITIES
// ============================================

function setWithLRULimit(cache, key, value, maxSize) {
  if (cache.has(key)) {
    cache.delete(key);
  }
  
  if (cache.size >= maxSize) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
  
  cache.set(key, value);
}

function getWithLRURefresh(cache, key) {
  if (!cache.has(key)) {
    return null;
  }
  
  const value = cache.get(key);
  cache.delete(key);
  cache.set(key, value);
  return value;
}

app.use(helmet());
app.use(express.json({ limit: '30mb' }));
app.use(cors());

// ============================================
// WATERMARK POSITIONS AND TEXT RENDERING
// ============================================

const WATERMARK_POSITIONS = [
  { x: -37, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 9, containerWidth: 200, containerHeight: 130 },
  { x: 92.6, y: 167.4, containerWidth: 200, containerHeight: 130 },
  { x: 337.4, y: 167.4, containerWidth: 200, containerHeight: 130 },
  { x: -37, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 325.8, containerWidth: 200, containerHeight: 130 },
  { x: 92.6, y: 484.2, containerWidth: 200, containerHeight: 130 },
  { x: 337.4, y: 484.2, containerWidth: 200, containerHeight: 130 },
  { x: -37, y: 642.6, containerWidth: 200, containerHeight: 130 },
  { x: 207.8, y: 642.6, containerWidth: 200, containerHeight: 130 },
  { x: 452.6, y: 642.6, containerWidth: 200, containerHeight: 130 }
];

function getCenteredWatermarkWithText(container, logoWidth, logoHeight, textWidth, textHeight) {
  const logoX = container.x + (container.containerWidth - logoWidth) / 2;
  const logoY = container.y + (container.containerHeight - logoHeight - textHeight - 25) / 2 + textHeight + 25;
  const gap = 25;
  const textX = container.x + (container.containerWidth - textWidth) / 2 + 20;
  const textY = logoY - textHeight - gap;
  
  return { logoX, logoY, textX, textY };
}

async function renderTextAsImage(text, fontSize, color, opacity) {
  const cacheKey = `${text}:${fontSize}`;
  
  const cached = getWithLRURefresh(textImageCache, cacheKey);
  if (cached) {
    return cached;
  }
  
  const truncatedText = text.substring(0, MAX_FUNDER_NAME_LENGTH);
  
  const renderDpi = 300;
  const renderScale = renderDpi / 72;
  const scaledFontSize = fontSize * renderScale;
  
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

// ============================================
// METADATA UTILITIES
// ============================================

function addAquamarkMetadata(pdfDoc, userEmail, funderName = null) {
  const { PDFName, PDFString } = require('pdf-lib');
  
  const domain = userEmail.split('@')[1] || userEmail;
  const timestamp = new Date().toISOString();
  
  const infoDict = pdfDoc.getInfoDict();
  
  infoDict.set(PDFName.of('AquamarkProtected'), PDFString.of('true'));
  infoDict.set(PDFName.of('AquamarkBroker'), PDFString.of(domain));
  infoDict.set(PDFName.of('AquamarkTimestamp'), PDFString.of(timestamp));
  
  if (funderName) {
    infoDict.set(PDFName.of('AquamarkFunder'), PDFString.of(funderName));
  }
  
  const existingKeywords = infoDict.get(PDFName.of('Keywords'));
  const keywordsText = existingKeywords ? existingKeywords.toString() : '';
  
  const aquamarkKeywords = funderName 
    ? `AquamarkProtected: true, AquamarkBroker: ${domain}, AquamarkFunder: ${funderName}`
    : `AquamarkProtected: true, AquamarkBroker: ${domain}`;
    
  const newKeywords = keywordsText 
    ? `${keywordsText}, ${aquamarkKeywords}`
    : aquamarkKeywords;
  
  infoDict.set(PDFName.of('Keywords'), PDFString.of(newKeywords));
}

// ============================================
// CORE FUNCTIONS
// ============================================

async function getCachedLogo(brokerId) {
  const cached = getWithLRURefresh(logoCache, brokerId);
  if (cached) {
    return cached;
  }
  
  const { data: broker } = await supabase
    .from('broker_gateway_users')
    .select('logo_storage_path')
    .eq('broker_id', brokerId)
    .single();
  
  if (!broker || !broker.logo_storage_path) {
    throw new Error('No logo found for broker');
  }
  
  const { data: logoUrlData } = supabase.storage
    .from('gateway-logos')
    .getPublicUrl(broker.logo_storage_path);
  
  const logoRes = await fetch(logoUrlData.publicUrl);
  const logoBytes = Buffer.from(await logoRes.arrayBuffer());
  
  setWithLRULimit(logoCache, brokerId, logoBytes, MAX_LOGO_CACHE_SIZE);
  return logoBytes;
}

function cleanupTempFiles(...files) {
  for (const file of files) {
    try {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file);
      }
    } catch (err) {
      logger.error('Temp file cleanup failed', { file, error: err.message });
    }
  }
}

async function watermarkPdf(pdfBuffer, logoBytes, userEmail, funderName = null) {
  const tempId = crypto.randomUUID();
  const inPath = path.join('/tmp', `temp-${tempId}-in.pdf`);
  const cleanedPath = path.join('/tmp', `temp-${tempId}-clean.pdf`);
  
  let cleanedPdfBytes;
  
  try {
    fs.writeFileSync(inPath, pdfBuffer);
    
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("PDF processing timeout")), 15000);
      
      exec(`qpdf --decrypt "${inPath}" "${cleanedPath}"`, (error, stdout, stderr) => {
        clearTimeout(timeout);
        
        if (fs.existsSync(cleanedPath) && fs.statSync(cleanedPath).size > 0) {
          resolve();
        } else {
          reject(new Error(`Unable to process PDF: ${stderr || error?.message || 'Unknown error'}`));
        }
      });
    });
    
    cleanedPdfBytes = fs.readFileSync(cleanedPath);
    cleanupTempFiles(inPath, cleanedPath);
    
    const pdfDoc = await PDFDocument.load(cleanedPdfBytes, {
      ignoreEncryption: true,
      updateMetadata: false,
      throwOnInvalidObject: false
    });
    
    cleanedPdfBytes = null;
    
    const { width, height } = pdfDoc.getPages()[0].getSize();
    
    // If funderName is provided, use the funder watermarking method
    if (funderName) {
      return await createWatermarkedPdfForFunder(pdfDoc, funderName, logoBytes, width, height, userEmail);
    }
    
    // Otherwise, use standard broker watermarking (logo only)
    return await createStandardWatermark(pdfDoc, logoBytes, width, height, userEmail);
    
  } catch (error) {
    cleanupTempFiles(inPath, cleanedPath);
    throw error;
  }
}

async function createWatermarkedPdfForFunder(pdfDoc, funderName, logoBytes, pageWidth, pageHeight, userEmail) {
  const funderPdfDoc = await PDFDocument.create({ updateMetadata: false });
  
  const { PDFName } = require('pdf-lib');
  
  // Preserve original metadata
  const sourceInfoDict = pdfDoc.getInfoDict();
  const targetInfoDict = funderPdfDoc.getInfoDict();
  
  sourceInfoDict.entries().forEach(([key, value]) => {
    const keyName = key.toString();
    if (!keyName.includes('Aquamark')) {
      targetInfoDict.set(key, value);
    }
  });
  
  // Copy pages
  const copiedPages = await funderPdfDoc.copyPages(pdfDoc, pdfDoc.getPageIndices());
  for (const page of copiedPages) {
    funderPdfDoc.addPage(page);
  }
  
  // Create watermark
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
  const watermarkPage = watermarkDoc.addPage([pageWidth, pageHeight]);
  
  // Add logo + text watermarks
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
      rotate: { angle: Math.PI / 4, type: 'radians' }
    });
    
    watermarkPage.drawImage(watermarkLogo, {
      x: logoX,
      y: logoY,
      width: logoWidth,
      height: logoHeight,
      opacity: 0.30,
      rotate: { angle: Math.PI / 4, type: 'radians' }
    });
  }
  
  const watermarkPdfBytes = await watermarkDoc.save();
  const watermarkEmbed = await PDFDocument.load(watermarkPdfBytes);
  const [embeddedPage] = await funderPdfDoc.embedPages([watermarkEmbed.getPages()[0]]);
  
  const pages = funderPdfDoc.getPages();
  pages.forEach((page) => {
    const { width, height } = page.getSize();
    page.drawPage(embeddedPage, { x: 0, y: 0, width, height });
  });
  
  addAquamarkMetadata(funderPdfDoc, userEmail, funderName);
  
  return await funderPdfDoc.save({
    useObjectStreams: false,
    addDefaultPage: false,
    objectsPerTick: 50,
    updateMetadata: false
  });
}

async function createStandardWatermark(pdfDoc, logoBytes, pageWidth, pageHeight, userEmail) {
  // Standard broker watermark (logo only, no funder name)
  const watermarkDoc = await PDFDocument.create();
  const watermarkImage = await watermarkDoc.embedPng(logoBytes);
  const watermarkPage = watermarkDoc.addPage([pageWidth, pageHeight]);
  
  const logoWidth = 80;
  const logoHeight = (logoWidth / watermarkImage.width) * watermarkImage.height;
  
  const positions = [
    { x: 35, y: 45 },
    { x: 279.8, y: 45 },
    { x: 524.6, y: 45 },
    { x: 218.6, y: 203.4 },
    { x: 463.4, y: 203.4 },
    { x: 35, y: 361.8 },
    { x: 279.8, y: 361.8 },
    { x: 524.6, y: 361.8 },
    { x: 218.6, y: 520.2 },
    { x: 463.4, y: 520.2 },
    { x: 35, y: 678.6 },
    { x: 279.8, y: 678.6 },
    { x: 524.6, y: 678.6 }
  ];
  
  positions.forEach(pos => {
    watermarkPage.drawImage(watermarkImage, {
      x: pos.x,
      y: pos.y,
      width: logoWidth,
      height: logoHeight,
      opacity: 0.25,
      rotate: { type: 'degrees', angle: 45 }
    });
  });
  
  const watermarkPdfBytes = await watermarkDoc.save();
  const watermarkEmbed = await PDFDocument.load(watermarkPdfBytes);
  const [embeddedPage] = await pdfDoc.embedPages([watermarkEmbed.getPages()[0]]);
  
  pdfDoc.getPages().forEach((page) => {
    page.drawPage(embeddedPage, { x: 0, y: 0, width: pageWidth, height: pageHeight });
  });
  
  addAquamarkMetadata(pdfDoc, userEmail);
  
  return await pdfDoc.save();
}

async function trackUsage(userEmail, fileCount, pageCount) {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  
  const { data: existing } = await supabase
    .from('broker_monthly_usage')
    .select('*')
    .eq('user_email', userEmail)
    .eq('year', year)
    .eq('month', month)
    .single();
  
  if (existing) {
    await supabase
      .from('broker_monthly_usage')
      .update({
        file_count: existing.file_count + fileCount,
        page_count: existing.page_count + pageCount,
        updated_at: now.toISOString()
      })
      .eq('user_email', userEmail)
      .eq('year', year)
      .eq('month', month);
  } else {
    await supabase
      .from('broker_monthly_usage')
      .insert({
        user_email: userEmail,
        year,
        month,
        file_count: fileCount,
        page_count: pageCount,
        created_at: now.toISOString(),
        updated_at: now.toISOString()
      });
  }
}

async function sendEmailWithAttachments(toEmail, fromEmail, subject, attachments) {
  const response = await fetch('https://api.postmarkapp.com/email', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'X-Postmark-Server-Token': POSTMARK_API_KEY
    },
    body: JSON.stringify({
      From: fromEmail,
      To: toEmail,
      Subject: subject,
      TextBody: 'Your watermarked documents are attached.',
      Attachments: attachments
    })
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Failed to send email: ${error}`);
  }
  
  return await response.json();
}

// ============================================
// POSTMARK INBOUND WEBHOOK
// ============================================

app.post("/inbound", async (req, res) => {
  try {
    const { FromFull, ToFull, Subject, Attachments } = req.body;
    
    if (!FromFull || !ToFull || !Attachments || Attachments.length === 0) {
      logger.warn('Invalid inbound email', { from: FromFull?.Email, to: ToFull?.Email });
      return res.status(400).send('Invalid email');
    }
    
    const fromEmail = FromFull.Email;
    const toEmail = ToFull.Email;
    
    // Extract broker_id from email: broker-fusion@gateway.aquamark.io -> fusion
    const emailMatch = toEmail.match(/^broker-([^@]+)@/);
    if (!emailMatch) {
      logger.warn('Invalid gateway email format', { toEmail });
      return res.status(400).send('Invalid gateway email');
    }
    
    const brokerId = emailMatch[1];
    
    // Get broker info
    const { data: broker, error } = await supabase
      .from('broker_gateway_users')
      .select('*')
      .eq('broker_id', brokerId)
      .eq('active', true)
      .single();
    
    if (error || !broker) {
      logger.error('Broker not found', { brokerId, error });
      return res.status(404).send('Broker not found');
    }
    
    // Verify sender email matches
    if (fromEmail !== broker.source_email) {
      logger.warn('Unauthorized sender', { fromEmail, expected: broker.source_email });
      return res.status(403).send('Unauthorized sender');
    }
    
    logger.info('Processing inbound email', { 
      brokerId, 
      fromEmail, 
      attachmentCount: Attachments.length,
      subject: Subject
    });
    
    // Parse funder names from subject (comma-separated)
    const funderNames = Subject && Subject.trim() 
      ? Subject.split(',').map(f => f.trim()).filter(f => f.length > 0)
      : [];
    
    logger.info('Funder names parsed', { funderNames, count: funderNames.length });
    
    // Get logo
    const logoBytes = await getCachedLogo(brokerId);
    
    // Process attachments
    const watermarkedAttachments = [];
    let totalPageCount = 0;
    let totalFileCount = 0;
    
    for (const attachment of Attachments) {
      if (!attachment.Name.toLowerCase().endsWith('.pdf')) {
        logger.warn('Skipping non-PDF attachment', { name: attachment.Name });
        continue;
      }
      
      const pdfBuffer = Buffer.from(attachment.Content, 'base64');
      
      if (pdfBuffer.length > MAX_FILE_SIZE) {
        logger.warn('Attachment too large', { name: attachment.Name, size: pdfBuffer.length });
        continue;
      }
      
      const baseName = attachment.Name.replace(/\.pdf$/i, '');
      
      // If no funders specified, create one version with standard watermark
      if (funderNames.length === 0) {
        const watermarkedPdf = await watermarkPdf(pdfBuffer, logoBytes, broker.source_email);
        
        // Count pages
        const pdfDoc = await PDFDocument.load(watermarkedPdf, { updateMetadata: false });
        totalPageCount += pdfDoc.getPageCount();
        totalFileCount++;
        
        watermarkedAttachments.push({
          Name: `${baseName}-protected.pdf`,
          Content: Buffer.from(watermarkedPdf).toString('base64'),
          ContentType: 'application/pdf'
        });
      } else {
        // Create a version for each funder
        for (const funderName of funderNames) {
          const watermarkedPdf = await watermarkPdf(pdfBuffer, logoBytes, broker.source_email, funderName);
          
          // Count pages
          const pdfDoc = await PDFDocument.load(watermarkedPdf, { updateMetadata: false });
          totalPageCount += pdfDoc.getPageCount();
          totalFileCount++;
          
          const funderSlug = funderName.substring(0, MAX_FUNDER_NAME_LENGTH)
            .replace(/\s+/g, '-').toLowerCase();
          
          watermarkedAttachments.push({
            Name: `${baseName}-${funderSlug}.pdf`,
            Content: Buffer.from(watermarkedPdf).toString('base64'),
            ContentType: 'application/pdf'
          });
        }
      }
    }
    
    if (watermarkedAttachments.length === 0) {
      logger.warn('No valid PDFs processed');
      return res.status(400).send('No valid PDFs to process');
    }
    
    // Send email back to broker
    await sendEmailWithAttachments(
      broker.destination_email,
      'gateway@aquamark.io',
      funderNames.length > 0 
        ? `Watermarked Documents - ${funderNames.join(', ')}`
        : 'Watermarked Documents',
      watermarkedAttachments
    );
    
    // Track usage
    await trackUsage(broker.source_email, totalFileCount, totalPageCount);
    
    logger.info('Email processed successfully', { 
      brokerId, 
      filesProcessed: totalFileCount,
      pagesProcessed: totalPageCount
    });
    
    res.status(200).send('OK');
    
  } catch (err) {
    logger.error('Error processing inbound email', { error: err.message, stack: err.stack });
    res.status(500).send('Internal server error');
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

app.listen(PORT, () => {
  logger.info('Broker Email Gateway started', { port: PORT });
  console.log(`🚀 Aquamark Broker Email Gateway on port ${PORT}`);
  console.log(`📧 Ready to receive emails at broker-{id}@gateway.aquamark.io`);
});
