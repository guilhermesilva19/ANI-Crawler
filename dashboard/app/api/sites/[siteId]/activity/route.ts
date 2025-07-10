import { NextResponse } from 'next/server';
import { getPerformanceHistory } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Get recent performance data - REAL-TIME crawling activity
    const perfHistory = await getPerformanceHistory();
    const recentPerf = await perfHistory.find({ 
      site_id: siteId 
    })
    .sort({ timestamp: -1 })
    .limit(50)  // Get more records to ensure fresh data
    .toArray();
    
    // Focus only on real-time performance data

    // ONLY USE REAL-TIME PERFORMANCE DATA
    const activities = recentPerf.map((perf: any) => ({
      id: perf._id,
      timestamp: perf.timestamp,
      type: 'crawl',
      action: perf.page_type === 'new' ? 'new_page' : 
              perf.page_type === 'changed' ? 'page_changed' :
              perf.page_type === 'failed' ? 'page_failed' : 'crawl_page',
      url: perf.url,
      details: `${(perf.crawl_time || 0).toFixed(1)}s crawl time`,
      status: perf.page_type === 'failed' ? 'error' :
              (perf.crawl_time || 0) > 10 ? 'warning' : 'success'
    }))
    .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
    .slice(0, 30);  // Show recent 30 activities
    
    return NextResponse.json({ activities });
  } catch (error) {
    console.error('Error fetching site activity:', error);
    return NextResponse.json(
      { error: 'Failed to fetch site activity' },
      { status: 500 }
    );
  }
} 