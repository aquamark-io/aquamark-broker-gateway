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
const AdmZip = require('adm-zip');

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

// Trust proxy for rate limiting (Render uses proxy)
app.set('trust proxy', 1);

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

// Initialize Postmark client
const postmarkClient = new postmark.ServerClient(process.env.POSTMARK_API_KEY);

const MAX_FUNDER_NAME_LENGTH = 20;
const textImageCache = new Map();
const MAX_TEXT_CACHE_SIZE = 1000;

// Cache utilities
function setWithLRULimit(cache, key, value, maxSize) {
  if (cache.has(key)) cache.delete(key);
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

// Watermark positions
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
  const textX = container.x + (container.containerWidth - textWidth) / 2 + 20 - 36; // Moved 36 points (0.5 inch) left
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
  
  const watermarkDoc = await PDFDocument.create();
  const { buffer: textImageBuffer, width: textWidth, height: textHeight } = await renderTextAsImage(
    funderName, 
    8, 
    { red: 0.8, green: 0.1, blue: 0.1 }, 
    0.5
  );
  
  const watermarkText = await watermarkDoc.embedPng(textImageBuffer);
  const watermarkPage = watermarkDoc.addPage([width, height]);
  
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
  
  pdfDoc.getPages().forEach((page) => {
    const { width, height } = page.getSize();
    page.drawPage(embeddedPage, { x: 0, y: 0, width, height });
  });
  
  const { PDFName, PDFString } = require('pdf-lib');
  const infoDict = pdfDoc.getInfoDict();
  infoDict.set(PDFName.of('AquamarkFunder'), PDFString.of(funderName));
  
  // Add to keywords
  const existingKeywords = infoDict.get(PDFName.of('Keywords'));
  const keywordsText = existingKeywords ? existingKeywords.toString().replace(/^\(|\)$/g, '') : '';
  
  const funderKeyword = `AquamarkFunder: ${funderName}`;
  const newKeywords = keywordsText 
    ? `${keywordsText}, ${funderKeyword}`
    : funderKeyword;
  
  infoDict.set(PDFName.of('Keywords'), PDFString.of(newKeywords));
  
  return await pdfDoc.save({ updateMetadata: false });
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'aquamark-broker-gateway' });
});

app.post('/inbound', async (req, res) => {
  try {
    logger.info('Received inbound email');
    
    const emailData = req.body;
    
    // Get TO address (gateway email) and FROM address (original sender)
    const toEmail = emailData.ToFull?.[0]?.Email || emailData.To;
    const fromEmail = emailData.From || emailData.FromFull?.Email;
    
    logger.info(`Email from: ${fromEmail} to: ${toEmail}`);
    
    // Validate gateway email format
    if (!toEmail || !toEmail.match(/@broker-gateway\.aquamark\.io$/i)) {
      logger.error(`Invalid gateway email format: ${toEmail}`);
      return res.status(400).json({ error: 'Invalid gateway email format' });
    }
    
    // Look up the gateway email in users table
    const { data: user, error: userError } = await supabase
      .from('users')
      .select('*')
      .eq('email', toEmail)
      .single();
    
    if (userError || !user) {
      logger.error(`Gateway email not authorized: ${toEmail}`);
      return res.status(404).json({ error: 'Gateway email not found or not authorized' });
    }
    
    logger.info(`Authorized gateway user: ${user.email}`);
    
    // Extract funder names from subject line
    // Subject line should contain comma-separated funder names: "Funder1, Funder2, Funder3"
    const subject = emailData.Subject || '';
    const funderNames = subject.trim() 
      ? subject.split(',').map(f => f.trim()).filter(f => f.length > 0)
      : [];
    
    logger.info(`Funders: ${funderNames.length > 0 ? funderNames.join(', ') : 'none'}`);
    
    // Get PDF attachments
    const pdfAttachments = (emailData.Attachments || []).filter(att => 
      att.ContentType === 'application/pdf' || 
      att.Name?.toLowerCase().endsWith('.pdf')
    );
    
    if (pdfAttachments.length === 0) {
      logger.warn('No PDF attachments');
      return res.status(200).json({ message: 'No PDFs to process' });
    }
    
    logger.info(`Processing ${pdfAttachments.length} PDF(s)`);
    
    const files = pdfAttachments.map(att => ({
      name: att.Name,
      data: att.Content
    }));
    
    try {
      logger.info('Calling Broker API');
      
      const watermarkResponse = await fetch(process.env.BROKER_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.BROKER_API_KEY}`
        },
        body: JSON.stringify({
          user_email: toEmail, // Use gateway email for Broker API authorization
          files: files,
          skip_usage_tracking: true // Gateway will track usage instead
        })
      });
      
      if (!watermarkResponse.ok) {
        const errorText = await watermarkResponse.text();
        logger.error('Broker API error:', { 
          status: watermarkResponse.status, 
          response: errorText.substring(0, 500) 
        });
        throw new Error(`Broker API failed with status ${watermarkResponse.status}`);
      }
      
      const contentType = watermarkResponse.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        const textResponse = await watermarkResponse.text();
        logger.error('Broker API returned non-JSON:', { 
          contentType, 
          response: textResponse.substring(0, 500) 
        });
        throw new Error('Broker API returned invalid response (not JSON)');
      }
      
      const result = await watermarkResponse.json();
      const jobId = result.job_id;
      
      logger.info(`Job created: ${jobId}, polling for completion`);
      const downloadUrl = await pollJobCompletion(jobId);
      
      logger.info(`Job complete, downloading from: ${downloadUrl}`);
      const brokerWatermarkedFiles = await downloadAndExtractFiles(downloadUrl);
      
      logger.info(`Downloaded ${brokerWatermarkedFiles.length} file(s)`);
      
      const finalFiles = [];
      let totalPageCount = 0;
      
      if (funderNames.length === 0) {
        // No funders - count broker-watermarked files
        for (const file of brokerWatermarkedFiles) {
          const pdfBuffer = Buffer.from(file.Content, 'base64');
          const pdfDoc = await PDFDocument.load(pdfBuffer, { updateMetadata: false });
          totalPageCount += pdfDoc.getPageCount();
        }
        finalFiles.push(...brokerWatermarkedFiles);
      } else {
        // With funders - only count the final funder-watermarked files
        for (const file of brokerWatermarkedFiles) {
          const pdfBuffer = Buffer.from(file.Content, 'base64');
          const baseName = file.Name.replace(/\.pdf$/i, '').replace(/-protected$/i, '');
          
          for (const funderName of funderNames) {
            logger.info(`Adding funder: ${funderName} to ${file.Name}`);
            
            const funderPdf = await addFunderWatermark(pdfBuffer, funderName);
            const funderSlug = funderName.substring(0, MAX_FUNDER_NAME_LENGTH)
              .replace(/\s+/g, '-').toLowerCase();
            
            // Count pages ONLY for funder files
            const pdfDoc = await PDFDocument.load(funderPdf, { updateMetadata: false });
            totalPageCount += pdfDoc.getPageCount();
            
            finalFiles.push({
              Name: `${baseName}-${funderSlug}.pdf`,
              Content: Buffer.from(funderPdf).toString('base64'),
              ContentType: 'application/pdf'
            });
          }
        }
      }
      
      logger.info(`Sending ${finalFiles.length} file(s) to ${fromEmail}`);
      
      // Send watermarked files back to original sender (FROM address)
      await postmarkClient.sendEmail({
        From: 'Aquamark <gateway@aquamark.io>',
        To: fromEmail, // Send back to original sender, not gateway email
        Subject: funderNames.length > 0 
          ? `Watermarked Documents - ${funderNames.join(', ')}`
          : 'Watermarked Documents',
        TextBody: 'Your watermarked documents are attached.',
        Attachments: finalFiles
      });
      
      // Track usage by gateway email (the broker identity)
      await trackUsage(toEmail, finalFiles.length, totalPageCount);
      
      logger.info('Email sent successfully');
      
      res.json({ 
        success: true, 
        files_sent: finalFiles.length
      });
      
    } catch (error) {
      logger.error('Processing error:', { 
        message: error.message,
        stack: error.stack,
        name: error.name
      });
      return res.status(500).json({ error: error.message });
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
  
  if (contentType && contentType.includes('application/pdf')) {
    const urlParts = downloadUrl.split('/');
    const filename = urlParts[urlParts.length - 1] || 'document.pdf';
    
    return [{
      Name: filename,
      Content: Buffer.from(buffer).toString('base64'),
      ContentType: 'application/pdf'
    }];
  }
  
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

async function trackUsage(userEmail, fileCount, pageCount) {
  try {
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
    
    logger.info(`Usage tracked: ${fileCount} files, ${pageCount} pages for ${userEmail}`);
  } catch (error) {
    logger.error('Usage tracking error:', error.message);
  }
}

app.listen(PORT, '0.0.0.0', () => {
  logger.info(`Broker Email Gateway running on port ${PORT}`);
  console.log(`🚀 Aquamark Broker Email Gateway on port ${PORT}`);
  console.log(`📧 Ready to receive emails at any @broker-gateway.aquamark.io address`);
});
