const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const winston = require('winston');
const { createClient } = require('@supabase/supabase-js');
const postmark = require('postmark');
const fetch = require('node-fetch');
const { PDFDocument } = require('pdf-lib');
const sharp = require('sharp');
const crypto = require('crypto');

// Initialize logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
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

// Initialize Express
const app = express();
const PORT = process.env.PORT || 10000;

// Security and middleware
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100
});
app.use('/inbound', limiter);

// Initialize Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Initialize Postmark client for sending
const postmarkClient = new postmark.ServerClient(process.env.POSTMARK_API_KEY);

const MAX_FUNDER_NAME_LENGTH = 20;
const textImageCache = new Map();
const MAX_TEXT_CACHE_SIZE = 1000;

// Cache management utilities
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
  if (!cache.has(key)) return null;
  const value = cache.get(key);
  cache.delete(key);
  cache.set(key, value);
  return value;
}

// Watermark positions (same as leak detection API)
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

function getCenteredWatermarkText(container, textWidth, textHeight) {
  const textX = container.x + (container.containerWidth - textWidth) / 2 + 20;
  const textY = container.y + (container.containerHeight - textHeight) / 2;
  return { textX, textY };
}

async function renderTextAsImage(text, fontSize, color, opacity) {
  const cacheKey = `${text}:${fontSize}`;
  const cached = getWithLRURefresh(textImageCache, cacheKey);
  if (cached) return cached;
  
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

async function addFunderWatermark(pdfBuffer, funderName) {
  const pdfDoc = await PDFDocument.load(pdfBuffer, { updateMetadata: false });
  const { width, height } = pdfDoc.getPages()[0].getSize();
  
  // Create watermark doc
  const watermarkDoc = await PDFDocument.create();
  const { buffer: textImageBuffer, width: textWidth, height: textHeight } = await renderTextAsImage(
    funderName, 
    8, 
    { red: 0.8, green: 0.1, blue: 0.1 }, 
    0.5
  );
  
  const watermarkText = await watermarkDoc.embedPng(textImageBuffer);
  const watermarkPage = watermarkDoc.addPage([width, height]);
  
  // Add funder text watermarks
  for (const container of WATERMARK_POSITIONS) {
    const { textX, textY } = getCenteredWatermarkText(container, textWidth, textHeight);
    
    watermarkPage.drawImage(watermarkText, {
      x: textX,
      y: textY,
      width: textWidth,
      height: textHeight,
      opacity: 0.30,
      rotate: { angle: Math.PI / 4, type: 'radians' }
    });
  }
  
  const watermarkPdfBytes = await watermarkDoc.save();
  const watermarkEmbed = await PDFDocument.load(watermarkPdfBytes);
  const [embeddedPage] = await pdfDoc.embedPages([watermarkEmbed.getPages()[0]]);
  
  // Apply to all pages
  pdfDoc.getPages().forEach((page) => {
    const { width, height } = page.getSize();
    page.drawPage(embeddedPage, { x: 0, y: 0, width, height });
  });
  
  // Add metadata
  const { PDFName, PDFString } = require('pdf-lib');
  const infoDict = pdfDoc.getInfoDict();
  infoDict.set(PDFName.of('AquamarkFunder'), PDFString.of(funderName));
  
  return await pdfDoc.save({ updateMetadata: false });
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'aquamark-broker-gateway' });
});

// Main inbound webhook endpoint
app.post('/inbound', async (req, res) => {
  try {
    logger.info('Received inbound email webhook');
    
    const emailData = req.body;
    
    // Extract broker ID from recipient address
    const recipientEmail = emailData.ToFull?.[0]?.Email || emailData.To;
    const brokerIdMatch = recipientEmail.match(/^broker-([^@]+)@/);
    
    if (!brokerIdMatch) {
      logger.error(`Invalid email format: ${recipientEmail}`);
      return res.status(400).json({ error: 'Invalid email format' });
    }
    
    const brokerId = brokerIdMatch[1];
    logger.info(`Processing email for broker: ${brokerId}`);
    
    // Look up broker configuration
    const { data: broker, error: brokerError } = await supabase
      .from('broker_gateway_users')
      .select('*')
      .eq('broker_id', brokerId)
      .eq('active', true)
      .single();
    
    if (brokerError || !broker) {
      logger.error(`Broker not found: ${brokerId}`, { error: brokerError });
      return res.status(404).json({ error: 'Broker not found' });
    }
    
    // Verify sender
    const senderEmail = emailData.From || emailData.FromFull?.Email;
    if (senderEmail !== broker.source_email) {
      logger.warn(`Unauthorized sender: ${senderEmail}`);
      return res.status(403).json({ error: 'Unauthorized sender' });
    }
    
    logger.info(`Found broker: ${broker.company_name}`);
    
    // Extract funder names from subject
    const subject = emailData.Subject || '';
    const funderNames = subject.trim() 
      ? subject.split(',').map(f => f.trim()).filter(f => f.length > 0)
      : [];
    
    logger.info(`Funder names: ${funderNames.length > 0 ? funderNames.join(', ') : 'none'}`);
    
    // Check for PDF attachments
    const attachments = emailData.Attachments || [];
    const pdfAttachments = attachments.filter(att => 
      att.Name.toLowerCase().endsWith('.pdf')
    );
    
    if (pdfAttachments.length === 0) {
      logger.warn('No PDF attachments found');
      return res.status(200).json({ message: 'No PDF attachments to process' });
    }
    
    logger.info(`Processing ${pdfAttachments.length} PDF(s)`);
    
    // Prepare files for Broker API
    const files = pdfAttachments.map(att => ({
      name: att.Name,
      data: att.Content
    }));
    
    try {
      // Step 1: Call Broker API to get broker-watermarked PDFs
      logger.info('Calling Broker API for standard watermarking');
      
      const watermarkPayload = {
        user_email: broker.source_email,
        files: files
      };
      
      const watermarkResponse = await fetch(process.env.BROKER_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.BROKER_API_KEY}`
        },
        body: JSON.stringify(watermarkPayload)
      });
      
      if (!watermarkResponse.ok) {
        const errorText = await watermarkResponse.text();
        throw new Error(`Broker API failed: ${errorText}`);
      }
      
      const result = await watermarkResponse.json();
      const jobId = result.job_id;
      
      // Poll for completion
      logger.info(`Polling job ${jobId}`);
      const downloadUrl = await pollJobCompletion(jobId);
      
      // Download broker-watermarked files
      const brokerWatermarkedFiles = await downloadAndExtractFiles(downloadUrl);
      
      // Step 2: If funder names provided, add funder watermarks on top
      const finalFiles = [];
      
      if (funderNames.length === 0) {
        // No funders - just use broker-watermarked files
        finalFiles.push(...brokerWatermarkedFiles);
      } else {
        // Add funder watermarks to each file for each funder
        for (const file of brokerWatermarkedFiles) {
          const pdfBuffer = Buffer.from(file.Content, 'base64');
          const baseName = file.Name.replace(/\.pdf$/i, '').replace(/-protected$/i, '');
          
          for (const funderName of funderNames) {
            logger.info(`Adding funder watermark: ${funderName} to ${file.Name}`);
            
            const funderWatermarkedBuffer = await addFunderWatermark(pdfBuffer, funderName);
            
            const funderSlug = funderName.substring(0, MAX_FUNDER_NAME_LENGTH)
              .replace(/\s+/g, '-').toLowerCase();
            
            finalFiles.push({
              Name: `${baseName}-${funderSlug}.pdf`,
              Content: Buffer.from(funderWatermarkedBuffer).toString('base64'),
              ContentType: 'application/pdf'
            });
          }
        }
      }
      
      logger.info(`Sending ${finalFiles.length} file(s) to ${broker.destination_email}`);
      
      // Send email
      const emailOptions = {
        From: 'Aquamark <gateway@aquamark.io>',
        To: broker.destination_email,
        Subject: funderNames.length > 0 
          ? `Watermarked Documents - ${funderNames.join(', ')}`
          : 'Watermarked Documents',
        TextBody: 'Your watermarked documents are attached.',
        Attachments: finalFiles
      };
      
      await postmarkClient.sendEmail(emailOptions);
      
      logger.info('Email sent successfully');
      
      res.json({ 
        success: true, 
        message: 'Email processed',
        files_sent: finalFiles.length
      });
      
    } catch (error) {
      logger.error('Processing error:', { 
        message: error.message, 
        stack: error.stack
      });
      return res.status(500).json({ 
        error: `Processing failed: ${error.message}` 
      });
    }
    
  } catch (error) {
    logger.error('Webhook error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

async function pollJobCompletion(jobId) {
  let attempts = 0;
  const maxAttempts = 30;
  
  while (attempts < maxAttempts) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const statusUrl = `${process.env.BROKER_API_URL.replace('/watermark', '')}/job-status/${jobId}`;
    const statusResponse = await fetch(statusUrl, {
      headers: { 'Authorization': `Bearer ${process.env.BROKER_API_KEY}` }
    });
    
    if (statusResponse.ok) {
      const status = await statusResponse.json();
      
      if (status.status === 'complete') {
        return status.download_url;
      } else if (status.status === 'error') {
        throw new Error(`Job failed: ${status.error_message}`);
      }
    }
    
    attempts++;
  }
  
  throw new Error('Job timed out');
}

async function downloadAndExtractFiles(downloadUrl) {
  const downloadResponse = await fetch(downloadUrl);
  if (!downloadResponse.ok) {
    throw new Error(`Download failed: ${downloadResponse.statusText}`);
  }
  
  const buffer = await downloadResponse.arrayBuffer();
  const contentType = downloadResponse.headers.get('content-type');
  
  // Single PDF
  if (contentType && contentType.includes('application/pdf')) {
    const urlParts = downloadUrl.split('/');
    const filename = urlParts[urlParts.length - 1] || 'document.pdf';
    
    return [{
      Name: filename,
      Content: Buffer.from(buffer).toString('base64'),
      ContentType: 'application/pdf'
    }];
  }
  
  // ZIP file
  const AdmZip = require('adm-zip');
  const zip = new AdmZip(Buffer.from(buffer));
  const zipEntries = zip.getEntries();
  
  return zipEntries
    .filter(entry => !entry.isDirectory && entry.entryName.toLowerCase().endsWith('.pdf'))
    .map(entry => ({
      Name: entry.entryName,
      Content: entry.getData().toString('base64'),
      ContentType: 'application/pdf'
    }));
}

app.listen(PORT, '0.0.0.0', () => {
  logger.info(`Broker Email Gateway running on port ${PORT}`);
  console.log(`🚀 Aquamark Broker Email Gateway on port ${PORT}`);
  console.log(`📧 Ready to receive emails at broker-{id}@gateway.aquamark.io`);
});

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
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

// Initialize Express
const app = express();
const PORT = process.env.PORT || 10000;

// Security and middleware
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100
});
app.use('/inbound', limiter);

// Initialize Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Initialize Postmark client for sending
const postmarkClient = new postmark.ServerClient(process.env.POSTMARK_API_KEY);

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'aquamark-broker-gateway' });
});

// Main inbound webhook endpoint
app.post('/inbound', async (req, res) => {
  try {
    logger.info('Received inbound email webhook');
    
    const emailData = req.body;
    
    // Extract broker ID from recipient address (e.g., broker-fusion@gateway.aquamark.io -> fusion)
    const recipientEmail = emailData.ToFull?.[0]?.Email || emailData.To;
    const brokerIdMatch = recipientEmail.match(/^broker-([^@]+)@/);
    
    if (!brokerIdMatch) {
      logger.error(`Invalid email format: ${recipientEmail}`);
      return res.status(400).json({ error: 'Invalid email format' });
    }
    
    const brokerId = brokerIdMatch[1];
    logger.info(`Processing email for broker: ${brokerId}`);
    
    // Look up broker configuration
    const { data: broker, error: brokerError } = await supabase
      .from('broker_gateway_users')
      .select('*')
      .eq('broker_id', brokerId)
      .eq('active', true)
      .single();
    
    if (brokerError || !broker) {
      logger.error(`Broker not found or inactive: ${brokerId}`, { error: brokerError });
      return res.status(404).json({ error: 'Broker not found' });
    }
    
    // Verify sender
    const senderEmail = emailData.From || emailData.FromFull?.Email;
    if (senderEmail !== broker.source_email) {
      logger.warn(`Unauthorized sender: ${senderEmail}, expected: ${broker.source_email}`);
      return res.status(403).json({ error: 'Unauthorized sender' });
    }
    
    logger.info(`Found broker: ${broker.company_name}`);
    
    // Extract subject for funder names
    const subject = emailData.Subject || '';
    const funderNames = subject.trim() 
      ? subject.split(',').map(f => f.trim()).filter(f => f.length > 0)
      : [];
    
    logger.info(`Funder names from subject: ${funderNames.length > 0 ? funderNames.join(', ') : 'none'}`);
    
    // Check for attachments
    const attachments = emailData.Attachments || [];
    
    if (attachments.length === 0) {
      logger.warn('No attachments found in email');
      return res.status(200).json({ message: 'No attachments to process' });
    }
    
    // Filter to PDF attachments only
    const pdfAttachments = attachments.filter(att => 
      att.Name.toLowerCase().endsWith('.pdf')
    );
    
    if (pdfAttachments.length === 0) {
      logger.warn('No PDF attachments found in email');
      return res.status(200).json({ message: 'No PDF attachments to process' });
    }
    
    logger.info(`Processing ${pdfAttachments.length} PDF attachment(s)`);
    
    // Prepare files array for Broker API
    const files = pdfAttachments.map(att => ({
      name: att.Name,
      data: att.Content // Postmark provides base64
    }));
    
    try {
      // If funder names provided, we need to call the API multiple times (once per funder)
      // and collect all results
      const allWatermarkedFiles = [];
      
      if (funderNames.length === 0) {
        // No funders - call standard broker API
        logger.info(`Calling Broker API for standard watermarking`);
        
        const watermarkPayload = {
          user_email: broker.source_email,
          files: files
        };
        
        const watermarkResponse = await fetch(process.env.BROKER_API_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${process.env.BROKER_API_KEY}`
          },
          body: JSON.stringify(watermarkPayload)
        });
        
        if (!watermarkResponse.ok) {
          const errorText = await watermarkResponse.text();
          throw new Error(`Broker API failed (${watermarkResponse.status}): ${errorText}`);
        }
        
        const result = await watermarkResponse.json();
        const jobId = result.job_id;
        
        // Poll for completion
        const downloadUrl = await pollJobCompletion(jobId);
        
        // Download and extract files
        const extractedFiles = await downloadAndExtractFiles(downloadUrl);
        allWatermarkedFiles.push(...extractedFiles);
        
      } else {
        // Has funders - call leak detection API for each funder
        logger.info(`Calling Leak Detection API for ${funderNames.length} funder(s)`);
        
        for (const funderName of funderNames) {
          logger.info(`Processing funder: ${funderName}`);
          
          const watermarkPayload = {
            user_email: broker.source_email,
            funder_name: funderName,
            files: files
          };
          
          const watermarkResponse = await fetch(process.env.LEAK_DETECTION_API_URL, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${process.env.LEAK_DETECTION_API_KEY}`
            },
            body: JSON.stringify(watermarkPayload)
          });
          
          if (!watermarkResponse.ok) {
            const errorText = await watermarkResponse.text();
            logger.error(`Leak Detection API failed for ${funderName}: ${errorText}`);
            continue; // Skip this funder but continue with others
          }
          
          const result = await watermarkResponse.json();
          const jobId = result.job_id;
          
          // Poll for completion
          const downloadUrl = await pollJobCompletion(jobId);
          
          // Download and extract files
          const extractedFiles = await downloadAndExtractFiles(downloadUrl);
          allWatermarkedFiles.push(...extractedFiles);
        }
      }
      
      if (allWatermarkedFiles.length === 0) {
        throw new Error('No watermarked files were generated');
      }
      
      logger.info(`Sending ${allWatermarkedFiles.length} watermarked file(s) to ${broker.destination_email}`);
      
      // Send email with all watermarked files as individual attachments
      const emailOptions = {
        From: 'Aquamark <gateway@aquamark.io>',
        To: broker.destination_email,
        Subject: funderNames.length > 0 
          ? `Watermarked Documents - ${funderNames.join(', ')}`
          : 'Watermarked Documents',
        TextBody: 'Your watermarked documents are attached.',
        Attachments: allWatermarkedFiles
      };
      
      await postmarkClient.sendEmail(emailOptions);
      
      logger.info('Email sent successfully');
      
      res.json({ 
        success: true, 
        message: 'Email processed and sent',
        files_processed: allWatermarkedFiles.length
      });
      
    } catch (error) {
      logger.error(`Error during processing:`, { 
        message: error.message, 
        stack: error.stack
      });
      return res.status(500).json({ 
        error: `Processing failed: ${error.message}` 
      });
    }
    
  } catch (error) {
    logger.error('Error processing webhook:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Helper function to poll job completion
async function pollJobCompletion(jobId) {
  logger.info(`Polling job ${jobId} for completion...`);
  
  let attempts = 0;
  const maxAttempts = 30; // 60 seconds max (2 sec intervals)
  
  while (attempts < maxAttempts) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const statusUrl = `${process.env.BROKER_API_URL.replace('/watermark', '')}/job-status/${jobId}`;
    const statusResponse = await fetch(statusUrl, {
      headers: {
        'Authorization': `Bearer ${process.env.BROKER_API_KEY}`
      }
    });
    
    if (statusResponse.ok) {
      const status = await statusResponse.json();
      logger.info(`Job ${jobId} status: ${status.status}`);
      
      if (status.status === 'complete') {
        return status.download_url;
      } else if (status.status === 'error') {
        throw new Error(`Job failed: ${status.error_message}`);
      }
    }
    
    attempts++;
  }
  
  throw new Error('Job timed out');
}

// Helper function to download and extract files
async function downloadAndExtractFiles(downloadUrl) {
  logger.info(`Downloading from: ${downloadUrl}`);
  
  const downloadResponse = await fetch(downloadUrl);
  
  if (!downloadResponse.ok) {
    throw new Error(`Failed to download: ${downloadResponse.statusText}`);
  }
  
  const buffer = await downloadResponse.arrayBuffer();
  const contentType = downloadResponse.headers.get('content-type');
  
  // If it's a single PDF
  if (contentType && contentType.includes('application/pdf')) {
    // Extract filename from URL or use default
    const urlParts = downloadUrl.split('/');
    const filename = urlParts[urlParts.length - 1] || 'document.pdf';
    
    return [{
      Name: filename,
      Content: Buffer.from(buffer).toString('base64'),
      ContentType: 'application/pdf'
    }];
  }
  
  // If it's a ZIP, extract all PDFs
  const AdmZip = require('adm-zip');
  const zip = new AdmZip(Buffer.from(buffer));
  const zipEntries = zip.getEntries();
  
  return zipEntries
    .filter(entry => !entry.isDirectory && entry.entryName.toLowerCase().endsWith('.pdf'))
    .map(entry => ({
      Name: entry.entryName,
      Content: entry.getData().toString('base64'),
      ContentType: 'application/pdf'
    }));
}

// Start server
app.listen(PORT, '0.0.0.0', () => {
  logger.info(`Broker Email Gateway running on port ${PORT}`);
  console.log(`🚀 Aquamark Broker Email Gateway on port ${PORT}`);
  console.log(`📧 Ready to receive emails at broker-{id}@gateway.aquamark.io`);
});


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
