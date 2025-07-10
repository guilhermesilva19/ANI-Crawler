import { NextResponse } from 'next/server';
import { getPageChanges, getUrlStates } from '@/lib/mongodb';
import { ObjectId } from 'mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string; urlId: string }> }
) {
  try {
    const { siteId, urlId } = await params;
    
    // Get URL details first
    const urlStates = await getUrlStates();
    const urlDoc = await urlStates.findOne({ _id: new ObjectId(urlId) });
    
    if (!urlDoc || urlDoc.site_id !== siteId) {
      return NextResponse.json(
        { error: 'URL not found' },
        { status: 404 }
      );
    }
    
    // Get change history for this URL
    const pageChanges = await getPageChanges();
    const changes = await pageChanges.find({
      site_id: siteId,
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
    
    return NextResponse.json({
      url: urlDoc.url,
      changes: formattedChanges,
      totalChanges: changes.length
    });
  } catch (error) {
    console.error('Error fetching URL changes:', error);
    return NextResponse.json(
      { error: 'Failed to fetch URL changes' },
      { status: 500 }
    );
  }
} 