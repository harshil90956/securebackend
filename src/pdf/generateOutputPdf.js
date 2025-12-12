import puppeteer from 'puppeteer';

// A4 in px at 96 DPI (same as TicketOutputPreview.tsx uses)
const A4_WIDTH_PX = 794;
const A4_HEIGHT_PX = 1123;

let browserPromise = null;

async function getBrowser() {
  if (!browserPromise) {
    browserPromise = puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
  }

  return browserPromise;
}

/**
 * pages: [ { items: [ { type, ... } ] } ]
 * Each item:
 *  - image: { type: 'image', src, x, y, width, height }
 *  - text:  { type: 'text', text, x, y, fontSize }
 */
export async function generateOutputPdfBuffer(pages) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  try {

    await page.setViewport({ width: A4_WIDTH_PX, height: A4_HEIGHT_PX, deviceScaleFactor: 1 });
    // Disable navigation timeout so large/complex pages can finish rendering
    page.setDefaultNavigationTimeout(0);

    // Build HTML with one .page div per page, using absolute positioning
    const html = buildHtml(pages);

    // Use a lighter waitUntil and no timeout to avoid 30s navigation timeout
    await page.setContent(html, { waitUntil: 'load', timeout: 0 });

    const pdfBuffer = await page.pdf({
      width: '210mm',
      height: '297mm',
      margin: { top: '0mm', right: '0mm', bottom: '0mm', left: '0mm' },
      printBackground: true,
      preferCSSPageSize: true,
    });

    return pdfBuffer;
  } finally {
    try {
      await page.close();
    } catch (err) {
      if (err && err.code === 'EBUSY') {
        console.warn('Puppeteer page close EBUSY (ignoring):', err.path || err.message);
      } else {
        console.warn('Puppeteer page close error (ignored):', err);
      }
    }
  }
}

function escapeHtml(str = '') {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildHtml(pages = []) {
  const pageDivs = pages
    .map((page) => {
      const itemsHtml = (page.items || [])
        .map((item) => {
          if (item.type === 'image') {
            const src = item.src || '';
            return `
              <img
                src="${src}"
                style="position:absolute; left:${item.x}px; top:${item.y}px; width:${item.width}px; height:${item.height}px; object-fit:contain;"
              />
            `;
          }

          if (item.type === 'text') {
            const rawText = item.text || '';
            const baseFontSize = item.fontSize || 12;
            const fontFamily = item.fontFamily || 'Arial, sans-serif';

            if (Array.isArray(item.letterFontSizes) && item.letterFontSizes.length > 0) {
              const lettersHtml = Array.from(rawText).map((ch, idx) => {
                const safeChar = escapeHtml(ch);
                const size = item.letterFontSizes[idx] || baseFontSize;
                return `<span style="font-size:${size}px; font-family:${fontFamily}; white-space:pre;">${safeChar}</span>`;
              }).join('');

              return `
              <div
                style="position:absolute; left:${item.x}px; top:${item.y}px; font-size:${baseFontSize}px; font-family:${fontFamily}; color:#000; white-space:pre;"
              >${lettersHtml}</div>
            `;
            }

            const text = escapeHtml(rawText);
            return `
              <div
                style="position:absolute; left:${item.x}px; top:${item.y}px; font-size:${baseFontSize}px; font-family:${fontFamily}; color:#000; white-space:pre;"
              >${text}</div>
            `;
          }

          return '';
        })
        .join('\n');

      return `
        <div class="page">
          ${itemsHtml}
        </div>
      `;
    })
    .join('\n');

  return `
    <html>
      <head>
        <style>
          @page {
            size: A4;
            margin: 0;
          }
          html, body {
            margin: 0;
            padding: 0;
            width: ${A4_WIDTH_PX}px;
          }
          body {
            background: white;
          }
          .page {
            position: relative;
            width: ${A4_WIDTH_PX}px;
            height: ${A4_HEIGHT_PX}px;
            page-break-after: always;
            overflow: hidden;
          }
          .page:last-child {
            page-break-after: auto;
          }
          img {
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
          }
        </style>
      </head>
      <body>
        ${pageDivs}
      </body>
    </html>
  `;
}
