import { NextResponse } from 'next/server';
import { getUrlStates } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    const { searchParams } = new URL(request.url);
    
    // Pagination parameters
    const page = parseInt(searchParams.get('page') || '1');
    const limit = parseInt(searchParams.get('limit') || '50');
    const skip = (page - 1) * limit;
    
    // Filter parameters
    const status = searchParams.get('status'); // all, visited, remaining, deleted, failed
    const search = searchParams.get('search'); // URL search term
    const sortBy = searchParams.get('sortBy') || 'last_crawled'; // last_crawled, url, status
    const sortOrder = searchParams.get('sortOrder') || 'desc'; // asc, desc
    
    const urlStates = await getUrlStates();
    
    // Build MongoDB query
    const query: any = { site_id: siteId };
    
    // Status filters
    if (status === 'visited') {
      query.status = 'visited';
    } else if (status === 'remaining') {
      query.status = 'remaining';
    } else if (status === 'deleted') {
      query['status_info.status'] = { $in: [404, 410] };
      query['status_info.error_count'] = { $gte: 2 };
    } else if (status === 'failed') {
      query['status_info.status'] = { $gte: 400 };
    } else if (status === 'changed') {
      query.last_change = { $exists: true };
    }
    
    // Search filter
    if (search) {
      query.url = { $regex: search, $options: 'i' };
    }
    
    // Get total count for pagination
    const totalCount = await urlStates.countDocuments(query);
    
    // Build sort options
    const sortOptions: any = {};
    if (sortBy === 'url') {
      sortOptions.url = sortOrder === 'asc' ? 1 : -1;
    } else if (sortBy === 'status') {
      sortOptions.status = sortOrder === 'asc' ? 1 : -1;
    } else {
      // Default: sort by last crawled or first seen
      sortOptions['status_info.last_success'] = sortOrder === 'asc' ? 1 : -1;
      sortOptions.first_seen = sortOrder === 'asc' ? 1 : -1;
    }
    
    // Get URLs with pagination
    const urls = await urlStates
      .find(query)
      .sort(sortOptions)
      .skip(skip)
      .limit(limit)
      .toArray();
    
    // Format URLs for frontend
    const formattedUrls = urls.map((url: any) => ({
      id: url._id,
      url: url.url,
      status: url.status,
      statusCode: url.status_info?.status,
      lastSuccess: url.status_info?.last_success,
      errorCount: url.status_info?.error_count || 0,
      firstSeen: url.first_seen,
      lastCrawled: url.last_crawled,
      isDeleted: url.status_info?.status >= 400 && url.status_info?.error_count >= 2,
      isFailed: url.status_info?.status >= 400,
      isChanged: !!url.last_change  // Only true if there's actual change data stored
    }));
    
    // Calculate pagination info
    const totalPages = Math.ceil(totalCount / limit);
    const hasNextPage = page < totalPages;
    const hasPrevPage = page > 1;
    
    // Get status counts for filter badges
    const statusCounts = await Promise.all([
      urlStates.countDocuments({ site_id: siteId }), // total
      urlStates.countDocuments({ site_id: siteId, status: 'visited' }), // visited
      urlStates.countDocuments({ site_id: siteId, status: 'remaining' }), // remaining
      urlStates.countDocuments({ 
        site_id: siteId, 
        'status_info.status': { $in: [404, 410] },
        'status_info.error_count': { $gte: 2 }
      }), // deleted
      urlStates.countDocuments({ 
        site_id: siteId, 
        'status_info.status': { $gte: 400 }
      }), // failed
      urlStates.countDocuments({ 
        site_id: siteId, 
        last_change: { $exists: true }
      }), // changed
    ]);
    
    const response = {
      urls: formattedUrls,
      pagination: {
        currentPage: page,
        totalPages,
        totalCount,
        limit,
        hasNextPage,
        hasPrevPage
      },
      filters: {
        status,
        search,
        sortBy,
        sortOrder
      },
      statusCounts: {
        total: statusCounts[0],
        visited: statusCounts[1],
        remaining: statusCounts[2],
        deleted: statusCounts[3],
        failed: statusCounts[4],
        changed: statusCounts[5]
      }
    };
    
    return NextResponse.json(response);
  } catch (error) {
    console.error('Error fetching URLs:', error);
    return NextResponse.json(
      { error: 'Failed to fetch URLs' },
      { status: 500 }
    );
  }
} 