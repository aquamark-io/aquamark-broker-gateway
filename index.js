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

// Security and middleware
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Rate limiting - 100 emails per hour (25 per 15 min)
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 25,
  message: { error: 'Too many requests, please try again later' }
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
const MAX_FILE_SIZE = 25 * 1024 * 1024; // 25MB in bytes
const MAX_TOTAL_SIZE = 25 * 1024 * 1024; // 25MB total
const FETCH_TIMEOUT = 60000; // 60 seconds
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

// Sanitize funder name to prevent XSS/injection
function sanitizeFunderName(name) {
  if (!name || typeof name !== 'string') return '';
  
  // Remove any HTML/script tags, special chars that could cause issues
  return name
    .replace(/<[^>]*>/g, '') // Remove HTML tags
    .replace(/[<>'"&]/g, '') // Remove potential injection chars
    .substring(0, MAX_FUNDER_NAME_LENGTH)
    .trim();
}

// Validate PDF buffer
async function isValidPDF(buffer) {
  try {
    const pdfDoc = await PDFDocument.load(buffer, { 
      updateMetadata: false,
      ignoreEncryption: true 
    });
    return pdfDoc.getPageCount() > 0;
  } catch (error) {
    logger.warn('PDF validation failed:', error.message);
    return false;
  }
}

// Send error notification email
async function sendErrorEmail(destinationEmail, errorMessage, fileDetails = '') {
  try {
    await postmarkClient.sendEmail({
      From: 'Aquamark <gateway@aquamark.io>',
      To: destinationEmail,
      Subject: 'Watermarking Error - Action Required',
      TextBody: `We encountered an error processing your watermark request:

${errorMessage}

${fileDetails ? `Details: ${fileDetails}` : ''}

Please check your files and try again. If the issue persists, contact support@aquamark.io.

- Aquamark Team`,
      MessageStream: 'outbound'
    });
    logger.info('Error notification sent');
  } catch (error) {
    logger.error('Failed to send error email:', error.message);
  }
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
  const textX = container.x + (container.containerWidth - textWidth) / 2 + 20 - 36;
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
  
  const existingKeywords = infoDict.get(PDFName.of('Keywords'));
  const keywordsText = existingKeywords ? existingKeywords.toString().replace(/^\(|\)$/g, '') : '';
  
  const funderKeyword = `AquamarkFunder: ${funderName}`;
  const newKeywords = keywordsText 
    ? `${keywordsText}, ${funderKeyword}`
    : funderKeyword;
  
  infoDict.set(PDFName.of('Keywords'), PDFString.of(newKeywords));
  
  return await pdfDoc.save({ updateMetadata: false });
}

// Fetch with timeout using Promise.race
async function fetchWithTimeout(url, options = {}, timeout = FETCH_TIMEOUT) {
  return Promise.race([
    fetch(url, options),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Request timeout')), timeout)
    )
  ]);
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'aquamark-broker-gateway' });
});

app.post('/inbound', async (req, res) => {
  let broker = null;
  
  try {
    logger.info('Received inbound email');
    
    const emailData = req.body;
    const recipientEmail = emailData.ToFull?.[0]?.Email || emailData.To;
    
    // Extract broker ID from email
    const brokerIdMatch = recipientEmail.match(/^([^@]+)@broker-gateway\.aquamark\.io$/i);
    
    if (!brokerIdMatch) {
      logger.error(`Invalid email format: ${recipientEmail}`);
      return res.status(400).json({ error: 'Invalid email format' });
    }
    
    const brokerId = brokerIdMatch[1];
    logger.info(`Processing for broker: ${brokerId}`);
    
    const { data: brokerData, error: brokerError } = await supabase
      .from('broker_gateway_users')
      .select('*')
      .eq('broker_id', brokerId)
      .eq('active', true)
      .single();
    
    if (brokerError || !brokerData) {
      logger.error(`Broker not found: ${brokerId}`);
      return res.status(404).json({ error: 'Broker not found' });
    }
    
    broker = brokerData;
    
    // Case-insensitive email comparison
    const senderEmail = (emailData.From || emailData.FromFull?.Email || '').toLowerCase();
    const authorizedEmail = (broker.source_email || '').toLowerCase();
    
    if (senderEmail !== authorizedEmail) {
      logger.warn(`Unauthorized sender: ${senderEmail}`);
      return res.status(403).json({ error: 'Unauthorized' });
    }
    
    logger.info(`Found broker: ${broker.company_name}`);
    
    // Parse and sanitize funder names from subject
    const subject = emailData.Subject || '';
    const rawFunderNames = subject.trim() 
      ? subject.split(',').map(f => f.trim()).filter(f => f.length > 0)
      : [];
    
    const funderNames = rawFunderNames
      .map(name => sanitizeFunderName(name))
      .filter(name => name.length > 0);
    
    logger.info(`Funders: ${funderNames.length > 0 ? funderNames.join(', ') : 'none'}`);
    
    const attachments = emailData.Attachments || [];
    const pdfAttachments = attachments.filter(att => 
      att.Name.toLowerCase().endsWith('.pdf')
    );
    
    if (pdfAttachments.length === 0) {
      logger.warn('No PDF attachments');
      await sendErrorEmail(
        broker.destination_email,
        'No PDF attachments found in your email.',
        'Please attach PDF files and try again.'
      );
      return res.status(200).json({ message: 'No PDFs to process' });
    }
    
    // Validate file sizes
    let totalSize = 0;
    const oversizedFiles = [];
    
    for (const att of pdfAttachments) {
      const fileSize = Buffer.from(att.Content, 'base64').length;
      totalSize += fileSize;
      
      if (fileSize > MAX_FILE_SIZE) {
        oversizedFiles.push(`${att.Name} (${(fileSize / 1024 / 1024).toFixed(2)}MB)`);
      }
    }
    
    if (oversizedFiles.length > 0) {
      logger.error(`Files exceed 25MB limit: ${oversizedFiles.join(', ')}`);
      await sendErrorEmail(
        broker.destination_email,
        'One or more files exceed the 25MB size limit.',
        `Files too large: ${oversizedFiles.join(', ')}\n\nMaximum file size: 25MB per file`
      );
      return res.status(413).json({ error: 'File size limit exceeded' });
    }
    
    if (totalSize > MAX_TOTAL_SIZE) {
      logger.error(`Total size exceeds 25MB: ${(totalSize / 1024 / 1024).toFixed(2)}MB`);
      await sendErrorEmail(
        broker.destination_email,
        'Total attachment size exceeds 25MB limit.',
        `Total size: ${(totalSize / 1024 / 1024).toFixed(2)}MB\nMaximum total size: 25MB`
      );
      return res.status(413).json({ error: 'Total size limit exceeded' });
    }
    
    logger.info(`Processing ${pdfAttachments.length} PDF(s)`);
    
    // Validate PDFs before sending to API
    const validFiles = [];
    const corruptedFiles = [];
    
    for (const att of pdfAttachments) {
      const pdfBuffer = Buffer.from(att.Content, 'base64');
      const isValid = await isValidPDF(pdfBuffer);
      
      if (isValid) {
        validFiles.push({
          name: att.Name,
          data: att.Content
        });
      } else {
        corruptedFiles.push(att.Name);
      }
    }
    
    if (corruptedFiles.length > 0) {
      logger.warn(`Corrupted PDFs detected: ${corruptedFiles.join(', ')}`);
      
      if (validFiles.length === 0) {
        await sendErrorEmail(
          broker.destination_email,
          'All PDF files are corrupted or invalid.',
          `Corrupted files: ${corruptedFiles.join(', ')}\n\nPlease check your files and try again.`
        );
        return res.status(400).json({ error: 'All PDFs are corrupted' });
      }
      
      // Continue with valid files, notify about corrupted ones
      logger.info(`Continuing with ${validFiles.length} valid PDFs`);
    }
    
    try {
      logger.info('Calling Broker API');
      
      const watermarkResponse = await fetchWithTimeout(process.env.BROKER_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.BROKER_API_KEY}`
        },
        body: JSON.stringify({
          user_email: broker.source_email,
          files: validFiles,
          skip_usage_tracking: true
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
      const funderProcessingErrors = [];
      
      if (funderNames.length === 0) {
        // No funders - count broker-watermarked files
        for (const file of brokerWatermarkedFiles) {
          const pdfBuffer = Buffer.from(file.Content, 'base64');
          const pdfDoc = await PDFDocument.load(pdfBuffer, { updateMetadata: false });
          totalPageCount += pdfDoc.getPageCount();
        }
        finalFiles.push(...brokerWatermarkedFiles);
      } else {
        // With funders - add funder watermarks
        for (const file of brokerWatermarkedFiles) {
          const pdfBuffer = Buffer.from(file.Content, 'base64');
          const baseName = file.Name.replace(/\.pdf$/i, '').replace(/-protected$/i, '');
          
          for (const funderName of funderNames) {
            try {
              logger.info(`Adding funder: ${funderName} to ${file.Name}`);
              
              const funderPdf = await addFunderWatermark(pdfBuffer, funderName);
              const funderSlug = funderName.substring(0, MAX_FUNDER_NAME_LENGTH)
                .replace(/\s+/g, '-').toLowerCase();
              
              const pdfDoc = await PDFDocument.load(funderPdf, { updateMetadata: false });
              totalPageCount += pdfDoc.getPageCount();
              
              finalFiles.push({
                Name: `${baseName}-${funderSlug}.pdf`,
                Content: Buffer.from(funderPdf).toString('base64'),
                ContentType: 'application/pdf'
              });
            } catch (error) {
              logger.error(`Failed to add funder watermark: ${funderName} to ${file.Name}`, error);
              funderProcessingErrors.push(`${funderName} on ${file.Name}`);
            }
          }
        }
      }
      
      if (finalFiles.length === 0) {
        throw new Error('No files could be processed successfully');
      }
      
      logger.info(`Sending ${finalFiles.length} file(s)`);
      
      // Attempt to send email with retry
      let emailSent = false;
      let emailError = null;
      
      for (let attempt = 1; attempt <= 2; attempt++) {
        try {
          await postmarkClient.sendEmail({
            From: 'Aquamark <gateway@aquamark.io>',
            To: broker.destination_email,
            Subject: funderNames.length > 0 
              ? `Watermarked Documents - ${funderNames.join(', ')}`
              : 'Watermarked Documents',
            TextBody: `Your watermarked documents are attached.${
              corruptedFiles.length > 0 
                ? `\n\nNote: The following files were corrupted and skipped: ${corruptedFiles.join(', ')}` 
                : ''
            }${
              funderProcessingErrors.length > 0
                ? `\n\nWarning: Failed to add watermarks for: ${funderProcessingErrors.join(', ')}`
                : ''
            }`,
            Attachments: finalFiles
          });
          
          emailSent = true;
          logger.info(`Email sent successfully (attempt ${attempt})`);
          break;
        } catch (error) {
          emailError = error;
          logger.error(`Email send failed (attempt ${attempt}):`, error.message);
          
          if (attempt === 1) {
            await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2s before retry
          }
        }
      }
      
      if (!emailSent) {
        // Email failed after retries - send error notification
        await sendErrorEmail(
          broker.destination_email,
          'Failed to send watermarked documents after processing.',
          `Files were processed successfully but email delivery failed. Please contact support@aquamark.io.\n\nError: ${emailError.message}`
        );
        throw new Error(`Email delivery failed: ${emailError.message}`);
      }
      
      // Track usage ONLY after successful email delivery
      await trackUsage(broker.source_email, finalFiles.length, totalPageCount);
      
      logger.info('Processing complete');
      
      res.json({ 
        success: true, 
        files_sent: finalFiles.length,
        corrupted_files_skipped: corruptedFiles.length,
        funder_errors: funderProcessingErrors.length
      });
      
    } catch (error) {
      logger.error('Processing error:', { 
        message: error.message,
        stack: error.stack,
        name: error.name
      });
      
      // Send error notification to user
      if (broker) {
        await sendErrorEmail(
          broker.destination_email,
          'An error occurred while processing your watermark request.',
          `Error: ${error.message}\n\nPlease try again or contact support@aquamark.io if the issue persists.`
        );
      }
      
      return res.status(500).json({ error: error.message });
    }
    
  } catch (error) {
    logger.error('Webhook error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

async function pollJobCompletion(jobId) {
  let attempts = 0;
  const maxAttempts = 45; // 90 seconds max (45 × 2s)
  
  while (attempts < maxAttempts) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const statusUrl = `${process.env.BROKER_API_URL.replace('/watermark', '')}/job-status/${jobId}`;
    const statusResponse = await fetchWithTimeout(statusUrl, {
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
  
  throw new Error('Job timed out after 90 seconds');
}

async function downloadAndExtractFiles(downloadUrl) {
  const downloadResponse = await fetchWithTimeout(downloadUrl);
  if (!downloadResponse.ok) {
    throw new Error(`Download failed: ${downloadResponse.statusText}`);
  }
  
  const buffer = await downloadResponse.arrayBuffer();
  const contentType = downloadResponse.headers.get('content-type');
  
  if (contentType && contentType.includes('application/pdf')) {
    const urlParts = downloadUrl.split('/');
    const filename = urlParts[urlParts.length - 1] || 'document.pdf';
    
    // Validate the downloaded PDF
    const isValid = await isValidPDF(Buffer.from(buffer));
    if (!isValid) {
      throw new Error('Downloaded file is not a valid PDF');
    }
    
    return [{
      Name: filename,
      Content: Buffer.from(buffer).toString('base64'),
      ContentType: 'application/pdf'
    }];
  }
  
  const zip = new AdmZip(Buffer.from(buffer));
  const zipEntries = zip.getEntries();
  
  const validPdfs = [];
  
  for (const entry of zipEntries) {
    if (!entry.isDirectory && entry.entryName.toLowerCase().endsWith('.pdf')) {
      const pdfBuffer = entry.getData();
      const isValid = await isValidPDF(pdfBuffer);
      
      if (isValid) {
        validPdfs.push({
          Name: entry.entryName,
          Content: pdfBuffer.toString('base64'),
          ContentType: 'application/pdf'
        });
      } else {
        logger.warn(`Skipping invalid PDF from zip: ${entry.entryName}`);
      }
    }
  }
  
  if (validPdfs.length === 0) {
    throw new Error('No valid PDFs found in downloaded archive');
  }
  
  return validPdfs;
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
    
    logger.info(`Usage tracked: ${fileCount} files, ${pageCount} pages`);
  } catch (error) {
    logger.error('Usage tracking error:', error.message);
  }
}

app.listen(PORT, '0.0.0.0', () => {
  logger.info(`Broker Email Gateway running on port ${PORT}`);
  console.log(`🚀 Aquamark Broker Email Gateway on port ${PORT}`);
  console.log(`📧 Ready at {broker-id}@broker-gateway.aquamark.io`);
  console.log(`📊 Rate limit: 25 emails per 15 minutes (100/hour)`);
  console.log(`📦 Max file size: 25MB per file, 25MB total`);
});
