import { NextResponse } from 'next/server';
import { getPageChanges, getUrlStates } from '@/lib/mongodb';
import { ObjectId } from 'mongodb';
import crypto from 'crypto';

// Helper function to generate Drive URLs for any visited page
function generateDriveUrls(url: string) {
  // Generate safe filename like the crawler does
  const urlParsed = new URL(url);
  
  let pathPart = urlParsed.pathname.replace(/\//g, '_').replace(/^_+|_+$/g, '');
  const queryPart = urlParsed.search ? urlParsed.search.substring(1).replace(/&/g, '_').replace(/=/g, '-') : '';
  
  // Create base filename
  let baseName;
  if (pathPart) {
    baseName = `${urlParsed.hostname}_${pathPart}`;
  } else {
    baseName = `${urlParsed.hostname}_index`;
  }
  
  if (queryPart) {
    baseName += `_${queryPart}`;
  }
  
  // Add URL hash for uniqueness
  const urlHash = crypto.createHash('md5').update(url).digest('hex').substring(0, 8);
  const safeFilename = `${baseName}_${urlHash}`.substring(0, 100);
  
  // Generate Google Drive search URLs (these will open Drive search for the folder)
  const screenshotSearchUrl = `https://drive.google.com/drive/search?q="${safeFilename}" type:folder`;
  const htmlSearchUrl = `https://drive.google.com/drive/search?q="${safeFilename}" type:folder`;
  
  return {
    screenshot_url: screenshotSearchUrl,
    html_url: htmlSearchUrl
  };
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string; urlId: string }> }
) {
  try {
    const { siteId, urlId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    // Get URL details first
    const urlStates = await getUrlStates();
    const urlDoc = await urlStates.findOne({ _id: new ObjectId(urlId) });
    
    if (!urlDoc || urlDoc.site_id !== dbSiteId) {
      return NextResponse.json(
        { error: 'URL not found' },
        { status: 404 }
      );
    }
    
    // Get change history for this URL
    const pageChanges = await getPageChanges();
    const changes = await pageChanges.find({
      site_id: dbSiteId,
      url: urlDoc.url
    })
    .sort({ timestamp: -1 })
    .limit(20)
    .toArray();
    
    // Format changes for frontend
    const formattedChanges = changes.map((change: any) => ({
      id: change._id,
      timestamp: change.timestamp,
      url: change.url,
      changeDetails: change.change_details,
      summary: change.change_details?.change_summary || 'Page content changed'
    }));
    
    // Generate Drive URLs for this page (works for all visited pages)
    const driveUrls = generateDriveUrls(urlDoc.url);
    
    return NextResponse.json({
      url: urlDoc.url,
      changes: formattedChanges,
      totalChanges: changes.length,
      // Always include Drive URLs for visited pages
      driveUrls: driveUrls
    });
  } catch (error) {
    console.error('Error fetching URL changes:', error);
    return NextResponse.json(
      { error: 'Failed to fetch URL changes' },
      { status: 500 }
    );
  }
} 