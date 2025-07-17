'use client';

import { useState, useEffect } from 'react';
import { Search, Filter, ExternalLink, Clock, AlertTriangle, Trash2, Edit, Eye, X, Zap } from 'lucide-react';

interface URL {
  id: string;
  url: string;
  status: string;
  statusCode?: number;
  lastSuccess?: string;
  errorCount: number;
  firstSeen?: string;
  lastCrawled?: string;
  isDeleted: boolean;
  isFailed: boolean;
  isChanged: boolean;
}

interface URLBrowserProps {
  siteId: string;
}

interface Filters {
  status: string;
  search: string;
  sortBy: string;
  sortOrder: string;
  pageType: string;
}

interface StatusCounts {
  total: number;
  visited: number;
  remaining: number;
  in_progress: number;
  deleted: number;
  failed: number;
  changed: number;
}

export default function URLBrowser({ siteId }: URLBrowserProps) {
  const [urls, setUrls] = useState<URL[]>([]);
  const [loading, setLoading] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [statusCounts, setStatusCounts] = useState<StatusCounts>({
    total: 0,
    visited: 0,
    remaining: 0,
    in_progress: 0,
    deleted: 0,
    failed: 0,
    changed: 0
  });
  
  const [filters, setFilters] = useState<Filters>({
    status: 'all',
    search: '',
    sortBy: 'last_crawled',
    sortOrder: 'desc',
    pageType: 'all'
  });

  const [searchInput, setSearchInput] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  
  // Change history modal state
  const [selectedUrl, setSelectedUrl] = useState<URL | null>(null);
  const [changes, setChanges] = useState<any[]>([]);
  const [driveUrls, setDriveUrls] = useState<{screenshot_url: string, html_url: string} | null>(null);
  const [loadingChanges, setLoadingChanges] = useState(false);

  const fetchUrls = async (page = 1, newFilters = filters) => {
    setLoading(true);
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '50',
        status: newFilters.status,
        search: newFilters.search,
        sortBy: newFilters.sortBy,
        sortOrder: newFilters.sortOrder
      });

      const response = await fetch(`/api/sites/${siteId}/urls?${params}`);
      const data = await response.json();

      setUrls(data.urls);
      setCurrentPage(data.pagination.currentPage);
      setTotalPages(data.pagination.totalPages);
      setTotalCount(data.pagination.totalCount);
      setStatusCounts(data.statusCounts);
    } catch (error) {
      console.error('Error fetching URLs:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUrls(1, filters);
  }, [siteId, filters]);

  const handleSearch = () => {
    const newFilters = { ...filters, search: searchInput };
    setFilters(newFilters);
    setCurrentPage(1);
  };

  const handleFilterChange = (key: keyof Filters, value: string) => {
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    setCurrentPage(1);
  };

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    fetchUrls(page, filters);
  };

  const getStatusIcon = (url: URL) => {
    if (url.isDeleted) return <Trash2 className="w-4 h-4 text-red-500" />;
    if (url.isFailed) return <AlertTriangle className="w-4 h-4 text-yellow-500" />;
    if (url.isChanged) return <Edit className="w-4 h-4 text-blue-500" />;
    if (url.status === 'visited') return <Clock className="w-4 h-4 text-green-500" />;
    if (url.status === 'in_progress') return <Zap className="w-4 h-4 text-orange-500" />;
    return <Clock className="w-4 h-4 text-gray-500" />;
  };

  const getStatusText = (url: URL) => {
    if (url.isDeleted) return 'Deleted';
    if (url.isFailed) return 'Failed';
    if (url.isChanged) return 'Changed';
    if (url.status === 'visited') return 'Visited';
    if (url.status === 'in_progress') return 'In Progress';
    return 'Pending';
  };

  const getStatusColor = (url: URL) => {
    if (url.isDeleted) return 'text-red-500 bg-red-500/10';
    if (url.isFailed) return 'text-yellow-500 bg-yellow-500/10';
    if (url.isChanged) return 'text-blue-500 bg-blue-500/10';
    if (url.status === 'visited') return 'text-green-500 bg-green-500/10';
    if (url.status === 'in_progress') return 'text-orange-500 bg-orange-500/10';
    return 'text-gray-500 bg-gray-500/10';
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  const fetchChanges = async (url: URL) => {
    setLoadingChanges(true);
    setSelectedUrl(url);
    try {
      const response = await fetch(`/api/sites/${siteId}/urls/${url.id}/changes`);
      const data = await response.json();
      setChanges(data.changes || []);
      setDriveUrls(data.driveUrls || null);
    } catch (error) {
      console.error('Error fetching changes:', error);
      setChanges([]);
      setDriveUrls(null);
    } finally {
      setLoadingChanges(false);
    }
  };

  const closeChangesModal = () => {
    setSelectedUrl(null);
    setChanges([]);
    setDriveUrls(null);
  };

  // Helper function to determine if URL is a document
  const isDocumentUrl = (url: string) => {
    return url.includes('/download/') || url.toLowerCase().match(/\.(pdf|docx|xlsx|doc|xls|ppt|zip|rar|txt)$/);
  };

  // Filter URLs by page type
  const filteredUrls = urls.filter(url => {
    if (filters.pageType === 'all') return true;
    if (filters.pageType === 'document') return isDocumentUrl(url.url);
    if (filters.pageType === 'normal') return !isDocumentUrl(url.url);
    return true;
  });

  return (
    <div className="bg-gray-900 rounded-lg border border-gray-700 p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-white">URL Browser</h2>
        <div className="text-sm text-gray-400 font-mono">
          {filteredUrls.length.toLocaleString()} / {totalCount.toLocaleString()} URLs
        </div>
      </div>

      {/* Search and Filters */}
      <div className="mb-6 space-y-4">
        {/* Search Bar */}
        <div className="flex gap-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search URLs..."
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              className="w-full pl-10 pr-4 py-2 bg-gray-800 border border-gray-600 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={handleSearch}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Search
          </button>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition-colors flex items-center gap-2"
          >
            <Filter className="w-4 h-4" />
            Filters
          </button>
        </div>

        {/* Status Filter Badges */}
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => handleFilterChange('status', 'all')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'all'
                ? 'bg-blue-600 text-white border-blue-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            All ({statusCounts.total.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'visited')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'visited'
                ? 'bg-green-600 text-white border-green-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Visited ({statusCounts.visited.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'remaining')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'remaining'
                ? 'bg-gray-600 text-white border-gray-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Pending ({statusCounts.remaining.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'in_progress')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'in_progress'
                ? 'bg-orange-600 text-white border-orange-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            In Progress ({statusCounts.in_progress.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'deleted')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'deleted'
                ? 'bg-red-600 text-white border-red-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Deleted ({statusCounts.deleted.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'failed')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'failed'
                ? 'bg-yellow-600 text-white border-yellow-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Failed ({statusCounts.failed.toLocaleString()})
          </button>
          <button
            onClick={() => handleFilterChange('status', 'changed')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.status === 'changed'
                ? 'bg-blue-600 text-white border-blue-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Changed ({statusCounts.changed.toLocaleString()})
          </button>
        </div>

        {/* Page Type Filter */}
        <div className="flex flex-wrap gap-2 border-t border-gray-700 pt-3">
          <span className="text-xs text-gray-400 self-center mr-2">Type:</span>
          <button
            onClick={() => handleFilterChange('pageType', 'all')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.pageType === 'all'
                ? 'bg-purple-600 text-white border-purple-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            All Pages
          </button>
          <button
            onClick={() => handleFilterChange('pageType', 'normal')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.pageType === 'normal'
                ? 'bg-purple-600 text-white border-purple-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            HTML Pages
          </button>
          <button
            onClick={() => handleFilterChange('pageType', 'document')}
            className={`px-3 py-1 text-sm rounded-full border transition-colors ${
              filters.pageType === 'document'
                ? 'bg-purple-600 text-white border-purple-600'
                : 'bg-gray-800 text-gray-300 border-gray-600 hover:bg-gray-700'
            }`}
          >
            Documents
          </button>
        </div>

        {/* Advanced Filters */}
        {showFilters && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 bg-gray-800 rounded-lg border border-gray-600">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">Sort By</label>
              <select
                value={filters.sortBy}
                onChange={(e) => handleFilterChange('sortBy', e.target.value)}
                className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="last_crawled">Last Crawled</option>
                <option value="url">URL</option>
                <option value="status">Status</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">Sort Order</label>
              <select
                value={filters.sortOrder}
                onChange={(e) => handleFilterChange('sortOrder', e.target.value)}
                className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="desc">Newest First</option>
                <option value="asc">Oldest First</option>
              </select>
            </div>
          </div>
        )}
      </div>

      {/* URL List */}
      {loading ? (
        <div className="text-center py-8">
          <div className="text-gray-400">Loading URLs...</div>
        </div>
      ) : (
        <>
          <div className="space-y-2 mb-6">
            {filteredUrls.map((url) => (
              <div
                key={url.id}
                className="p-4 bg-gray-800 rounded-lg border border-gray-700 hover:bg-gray-750 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 mb-2">
                      {getStatusIcon(url)}
                      <span className={`px-2 py-1 text-xs rounded-full ${getStatusColor(url)}`}>
                        {getStatusText(url)}
                      </span>
                      {url.statusCode && (
                        <span className="px-2 py-1 text-xs bg-gray-700 text-gray-300 rounded font-mono">
                          {url.statusCode}
                        </span>
                      )}
                      {url.errorCount > 0 && (
                        <span className="px-2 py-1 text-xs bg-red-900 text-red-300 rounded">
                          {url.errorCount} errors
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-2 mb-2">
                      <a
                        href={url.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-blue-400 hover:text-blue-300 font-mono text-sm break-all flex items-center gap-1"
                      >
                        {url.url}
                        <ExternalLink className="w-3 h-3 flex-shrink-0" />
                      </a>
                    </div>
                    <div className="flex items-center gap-4 text-xs text-gray-400">
                      <span>First seen: {formatDate(url.firstSeen)}</span>
                      <span>Last crawled: {formatDate(url.lastCrawled)}</span>
                      {url.lastSuccess && (
                        <span>Last success: {formatDate(url.lastSuccess)}</span>
                      )}
                    </div>
                  </div>
                  
                  {/* Actions */}
                  <div className="flex items-center gap-2 ml-4">
                    {url.status === 'visited' && (
                      <button
                        onClick={() => fetchChanges(url)}
                        className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 transition-colors flex items-center gap-1"
                      >
                        <Eye className="w-3 h-3" />
                        View Details
                      </button>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* No results message */}
          {filteredUrls.length === 0 && urls.length > 0 && (
            <div className="text-center py-8">
              <div className="text-gray-400">No {filters.pageType === 'document' ? 'documents' : filters.pageType === 'normal' ? 'HTML pages' : 'URLs'} match your filters</div>
            </div>
          )}

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-400">
                Page {currentPage} of {totalPages} ({totalCount.toLocaleString()} total)
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                  className="px-3 py-1 bg-gray-700 text-white rounded hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Previous
                </button>
                
                {/* Page numbers */}
                <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                    let pageNum;
                    if (totalPages <= 5) {
                      pageNum = i + 1;
                    } else if (currentPage <= 3) {
                      pageNum = i + 1;
                    } else if (currentPage >= totalPages - 2) {
                      pageNum = totalPages - 4 + i;
                    } else {
                      pageNum = currentPage - 2 + i;
                    }
                    
                    return (
                      <button
                        key={pageNum}
                        onClick={() => handlePageChange(pageNum)}
                        className={`px-3 py-1 rounded ${
                          pageNum === currentPage
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                        }`}
                      >
                        {pageNum}
                      </button>
                    );
                  })}
                </div>

                <button
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                  className="px-3 py-1 bg-gray-700 text-white rounded hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </>
      )}

      {/* Change History Modal */}
      {selectedUrl && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg max-w-4xl max-h-[80vh] w-full mx-4 overflow-hidden">
            {/* Modal Header */}
            <div className="flex items-center justify-between p-4 border-b border-gray-700">
              <div className="flex-1 min-w-0">
                <h3 className="text-lg font-semibold text-white">Page Details</h3>
                <p className="text-sm text-gray-400 truncate font-mono mt-1">
                  {selectedUrl.url}
                </p>
              </div>
              <button
                onClick={closeChangesModal}
                className="p-2 hover:bg-gray-700 rounded-lg transition-colors"
              >
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            {/* Modal Content */}
            <div className="p-4 overflow-y-auto max-h-[60vh]">
              {loadingChanges ? (
                <div className="text-center py-8">
                  <div className="text-gray-400">Loading page details...</div>
                </div>
              ) : (
                <>
                  {/* Drive Links Section - Always show for visited pages */}
                  {driveUrls && (
                    <div className="mb-6 p-4 bg-gray-700 rounded-lg border border-gray-600">
                      <h4 className="text-sm font-medium text-gray-300 mb-3">📁 View Files</h4>
                      <div className="flex items-center gap-4">
                        {driveUrls.screenshot_url && (
                          <a
                            href={driveUrls.screenshot_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-2 px-3 py-2 bg-gray-800 rounded border border-gray-600 hover:border-blue-500 transition-colors"
                          >
                            <ExternalLink className="w-4 h-4" />
                            Screenshots
                          </a>
                        )}
                        {driveUrls.html_url && (
                          <a
                            href={driveUrls.html_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-2 px-3 py-2 bg-gray-800 rounded border border-gray-600 hover:border-blue-500 transition-colors"
                          >
                            <ExternalLink className="w-4 h-4" />
                            HTML Files
                          </a>
                        )}
                      </div>
                    </div>
                  )}
                  
                  {/* Changes Section */}
                  {changes.length === 0 ? (
                <div className="text-center py-8">
                      <div className="text-gray-400">No changes detected</div>
                  <div className="text-sm text-gray-500 mt-2">
                        This page has been crawled but no content changes have been detected yet.
                  </div>
                </div>
              ) : (
                    <>
                      <h4 className="text-sm font-medium text-gray-300 mb-4">📝 Change History</h4>
                <div className="space-y-4">
                  {changes.map((change, index) => (
                    <div key={change.id} className="bg-gray-900 rounded-lg p-4 border border-gray-600">
                      {/* Change Header */}
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <Edit className="w-4 h-4 text-blue-400" />
                          <span className="text-sm font-medium text-white">
                            Change #{changes.length - index}
                          </span>
                        </div>
                        <span className="text-xs text-gray-400">
                          {formatDate(change.timestamp)}
                        </span>
                      </div>

                      {/* Change Summary */}
                      <div className="mb-3">
                        <p className="text-sm text-gray-300">{change.summary}</p>
                      </div>

                      {/* Detailed Changes */}
                      {change.changeDetails && (
                        <div className="space-y-3">
                          {/* Added Text */}
                          {change.changeDetails.added_text?.length > 0 && (
                            <div className="bg-green-900/20 border border-green-700 rounded p-3">
                              <h4 className="text-sm font-medium text-green-400 mb-2">
                                ✅ Added Text ({change.changeDetails.added_text.length})
                              </h4>
                              <div className="space-y-1">
                                {change.changeDetails.added_text.slice(0, 3).map((item: any, i: number) => (
                                  <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-2 rounded">
                                    + {item.text}
                                  </p>
                                ))}
                                {change.changeDetails.added_text.length > 3 && (
                                  <p className="text-xs text-gray-400">
                                    ... and {change.changeDetails.added_text.length - 3} more
                                  </p>
                                )}
                              </div>
                            </div>
                          )}

                          {/* Deleted Text */}
                          {change.changeDetails.deleted_text?.length > 0 && (
                            <div className="bg-red-900/20 border border-red-700 rounded p-3">
                              <h4 className="text-sm font-medium text-red-400 mb-2">
                                ❌ Removed Text ({change.changeDetails.deleted_text.length})
                              </h4>
                              <div className="space-y-1">
                                {change.changeDetails.deleted_text.slice(0, 3).map((item: any, i: number) => (
                                  <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-2 rounded">
                                    - {item.text}
                                  </p>
                                ))}
                                {change.changeDetails.deleted_text.length > 3 && (
                                  <p className="text-xs text-gray-400">
                                    ... and {change.changeDetails.deleted_text.length - 3} more
                                  </p>
                                )}
                              </div>
                            </div>
                          )}

                          {/* Changed Text */}
                          {change.changeDetails.changed_text?.length > 0 && (
                            <div className="bg-yellow-900/20 border border-yellow-700 rounded p-3">
                              <h4 className="text-sm font-medium text-yellow-400 mb-2">
                                🔄 Modified Text ({change.changeDetails.changed_text.length})
                              </h4>
                              <div className="space-y-1">
                                {change.changeDetails.changed_text.slice(0, 3).map((item: any, i: number) => (
                                  <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-2 rounded">
                                    ~ {item.text}
                                  </p>
                                ))}
                                {change.changeDetails.changed_text.length > 3 && (
                                  <p className="text-xs text-gray-400">
                                    ... and {change.changeDetails.changed_text.length - 3} more
                                  </p>
                                )}
                              </div>
                            </div>
                          )}

                          {/* Link Changes */}
                          {(change.changeDetails.added_links?.length > 0 || change.changeDetails.removed_links?.length > 0) && (
                            <div className="bg-blue-900/20 border border-blue-700 rounded p-3">
                              <h4 className="text-sm font-medium text-blue-400 mb-2">🔗 Link Changes</h4>
                              <div className="space-y-2">
                                {change.changeDetails.added_links?.length > 0 && (
                                  <div>
                                    <p className="text-xs text-green-400 mb-1">Added ({change.changeDetails.added_links.length}):</p>
                                    {change.changeDetails.added_links.slice(0, 3).map((link: string, i: number) => (
                                      <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-1 rounded">
                                        + {link}
                                      </p>
                                    ))}
                                    {change.changeDetails.added_links.length > 3 && (
                                      <p className="text-xs text-gray-400">... and {change.changeDetails.added_links.length - 3} more</p>
                                    )}
                                  </div>
                                )}
                                {change.changeDetails.removed_links?.length > 0 && (
                                  <div>
                                    <p className="text-xs text-red-400 mb-1">Removed ({change.changeDetails.removed_links.length}):</p>
                                    {change.changeDetails.removed_links.slice(0, 3).map((link: string, i: number) => (
                                      <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-1 rounded">
                                        - {link}
                                      </p>
                                    ))}
                                    {change.changeDetails.removed_links.length > 3 && (
                                      <p className="text-xs text-gray-400">... and {change.changeDetails.removed_links.length - 3} more</p>
                                    )}
                                  </div>
                                )}
                              </div>
                            </div>
                          )}

                          {/* PDF Changes */}
                          {(change.changeDetails.added_pdfs?.length > 0 || change.changeDetails.removed_pdfs?.length > 0) && (
                            <div className="bg-purple-900/20 border border-purple-700 rounded p-3">
                              <h4 className="text-sm font-medium text-purple-400 mb-2">📄 PDF Changes</h4>
                              <div className="space-y-2">
                                {change.changeDetails.added_pdfs?.length > 0 && (
                                  <div>
                                    <p className="text-xs text-green-400 mb-1">Added ({change.changeDetails.added_pdfs.length}):</p>
                                    {change.changeDetails.added_pdfs.map((pdf: string, i: number) => (
                                      <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-1 rounded">
                                        + {pdf}
                                      </p>
                                    ))}
                                  </div>
                                )}
                                {change.changeDetails.removed_pdfs?.length > 0 && (
                                  <div>
                                    <p className="text-xs text-red-400 mb-1">Removed ({change.changeDetails.removed_pdfs.length}):</p>
                                    {change.changeDetails.removed_pdfs.map((pdf: string, i: number) => (
                                      <p key={i} className="text-xs text-gray-300 font-mono bg-gray-800 p-1 rounded">
                                        - {pdf}
                                      </p>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </div>
                          )}

                          {/* Drive Links */}
                          {(change.changeDetails.screenshot_url || change.changeDetails.html_url) && (
                            <div className="flex items-center gap-4 pt-2 border-t border-gray-600">
                              {change.changeDetails.screenshot_url && (
                                <a
                                  href={change.changeDetails.screenshot_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1"
                                >
                                  <ExternalLink className="w-3 h-3" />
                                  Screenshots
                                </a>
                              )}
                              {change.changeDetails.html_url && (
                                <a
                                  href={change.changeDetails.html_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1"
                                >
                                  <ExternalLink className="w-3 h-3" />
                                  HTML Files
                                </a>
                              )}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
                    </>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
} 