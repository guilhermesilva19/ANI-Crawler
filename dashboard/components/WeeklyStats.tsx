'use client';

import { useState, useEffect } from 'react';
import { CalendarDays, TrendingUp, Activity, AlertTriangle, BarChart3 } from 'lucide-react';

// Helper function to format deleted pages breakdown
function formatDeletedBreakdown(deletions: any): string {
  if (!deletions || deletions.deleted_this_week === 0) return 'no deletions this week';
  
  const parts = [];
  if (deletions.deleted_pdfs > 0) parts.push(`${deletions.deleted_pdfs} PDFs`);
  if (deletions.deleted_documents > 0) parts.push(`${deletions.deleted_documents} docs`);
  if (deletions.deleted_webpages > 0) parts.push(`${deletions.deleted_webpages} pages`);
  if (deletions.deleted_media > 0) parts.push(`${deletions.deleted_media} media`);
  if (deletions.deleted_archives > 0) parts.push(`${deletions.deleted_archives} archives`);
  
  return parts.length > 0 ? `${parts.join(', ')} • ${deletions.daily_average || 0}/day avg` : `pages deleted • ${deletions.daily_average || 0}/day avg`;
}

interface WeeklyStatsData {
  timeframe: string;
  period: string;
  timezone: string;
  discovery: {
    discovered_this_week: number;
    daily_average: number;
    description: string;
  };
  processing: {
    crawled_this_week: number;
    daily_average: number;
    new_processed: number;
    changed_processed: number;
    failed_processed: number;
    deleted_processed: number;
    document_processed: number;
    description: string;
  };
  changes: {
    changed_this_week: number;
    daily_average: number;
    description: string;
  };
  failures: {
    failed_this_week: number;
    daily_average: number;
    description: string;
  };
  deletions: {
    deleted_this_week: number;
    deleted_pdfs: number;
    deleted_documents: number;
    deleted_webpages: number;
    deleted_media: number;
    deleted_archives: number;
    daily_average: number;
    description: string;
  };
  daily_breakdown: Array<{
    date: string;
    stats: {
      pages_crawled: number;
      new_pages: number;
      changed_pages: number;
      failed_pages: number;
      deleted_pages: number;
      document_pages: number;
    };
  }>;
  current_totals: {
    total_pages: number;
    visited_pages: number;
    remaining_pages: number;
    failed_pages: number;
    deleted_pages: number;
    description: string;
  };
}

interface WeeklyStatsProps {
  siteId: string;
}

export default function WeeklyStats({ siteId }: WeeklyStatsProps) {
  const [data, setData] = useState<WeeklyStatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDailyBreakdown, setShowDailyBreakdown] = useState(false);

  useEffect(() => {
    if (siteId) {
      fetchData();
      
      // Auto-refresh every 10 minutes
      const interval = setInterval(fetchData, 10 * 60 * 1000);
      return () => clearInterval(interval);
    }
  }, [siteId]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(`/api/sites/${siteId}/weekly-stats`);
      
      if (!response.ok) {
        throw new Error('Failed to fetch weekly stats');
      }
      
      const result = await response.json();
      setData(result);
    } catch (err) {
      console.error('Error fetching weekly stats:', err);
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-AU', { 
      weekday: 'short', 
      month: 'short', 
      day: 'numeric' 
    });
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-700 rounded w-48 mb-4"></div>
          <div className="grid grid-cols-2 gap-4 mb-6">
            <div className="h-24 bg-gray-700 rounded"></div>
            <div className="h-24 bg-gray-700 rounded"></div>
            <div className="h-24 bg-gray-700 rounded"></div>
            <div className="h-24 bg-gray-700 rounded"></div>
          </div>
          <div className="h-32 bg-gray-700 rounded"></div>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="text-center text-red-400">
          <AlertTriangle className="w-8 h-8 mx-auto mb-2" />
          <p>Failed to load weekly stats</p>
          {error && <p className="text-sm text-gray-500 mt-1">{error}</p>}
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <CalendarDays className="w-5 h-5 text-purple-400" />
        <h3 className="text-lg font-medium text-white">📊 Weekly Stats</h3>
        <span className="text-sm text-gray-400">({data.period})</span>
      </div>

             {/* Main Stats Grid */}
       <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
        {/* Pages Discovered This Week */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="w-4 h-4 text-green-400" />
            <span className="text-sm text-gray-300">Discovered</span>
          </div>
                     <div className="text-2xl font-mono text-green-300 mb-1">
             {(data.discovery?.discovered_this_week || 0).toLocaleString()}
           </div>
           <div className="text-xs text-gray-500">
             pages found • {data.discovery?.daily_average || 0}/day avg
           </div>
        </div>

        {/* Pages Crawled This Week */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Activity className="w-4 h-4 text-blue-400" />
            <span className="text-sm text-gray-300">Processed</span>
          </div>
                     <div className="text-2xl font-mono text-blue-300 mb-1">
             {(data.processing?.crawled_this_week || 0).toLocaleString()}
           </div>
           <div className="text-xs text-gray-500">
             pages crawled • {data.processing?.daily_average || 0}/day avg
           </div>
        </div>

        {/* Changes Detected This Week */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="w-4 h-4 text-yellow-400" />
            <span className="text-sm text-gray-300">Changed</span>
          </div>
                     <div className="text-2xl font-mono text-yellow-300 mb-1">
             {(data.changes?.changed_this_week || 0).toLocaleString()}
           </div>
           <div className="text-xs text-gray-500">
             content changes • {data.changes?.daily_average || 0}/day avg
           </div>
        </div>

                 {/* Failures This Week */}
         <div className="bg-gray-700 rounded-lg p-4">
           <div className="flex items-center gap-2 mb-2">
             <AlertTriangle className="w-4 h-4 text-red-400" />
             <span className="text-sm text-gray-300">Failed</span>
           </div>
           <div className="text-2xl font-mono text-red-300 mb-1">
              {(data.failures?.failed_this_week || 0).toLocaleString()}
           </div>
           <div className="text-xs text-gray-500">
              load failures • {data.failures?.daily_average || 0}/day avg
           </div>
         </div>

         {/* Deletions This Week */}
         <div className="bg-gray-700 rounded-lg p-4">
           <div className="flex items-center gap-2 mb-2">
             <AlertTriangle className="w-4 h-4 text-red-500" />
             <span className="text-sm text-gray-300">Deleted</span>
           </div>
           <div className="text-2xl font-mono text-red-400 mb-1">
              {(data.deletions?.deleted_this_week || 0).toLocaleString()}
           </div>
                        <div className="text-xs text-gray-500">
               {formatDeletedBreakdown(data.deletions)}
             </div>
         </div>
      </div>

      {/* Processing Breakdown */}
      <div className="bg-gray-700 rounded-lg p-4 mb-6">
        <h4 className="text-sm font-medium text-gray-300 mb-3">Processing Breakdown (7 Days)</h4>
        <div className="grid grid-cols-3 gap-3 text-sm">
          <div className="text-center">
                         <div className="text-lg font-mono text-green-300">
               {(data.processing?.new_processed || 0).toLocaleString()}
             </div>
             <div className="text-xs text-gray-500">New</div>
           </div>
           <div className="text-center">
             <div className="text-lg font-mono text-yellow-300">
               {(data.processing?.changed_processed || 0).toLocaleString()}
             </div>
             <div className="text-xs text-gray-500">Changed</div>
           </div>
           <div className="text-center">
             <div className="text-lg font-mono text-blue-300">
               {(data.processing?.document_processed || 0).toLocaleString()}
             </div>
             <div className="text-xs text-gray-500">Documents</div>
           </div>
           <div className="text-center">
             <div className="text-lg font-mono text-red-300">
               {(data.processing?.failed_processed || 0).toLocaleString()}
             </div>
             <div className="text-xs text-gray-500">Failed</div>
           </div>
           <div className="text-center">
             <div className="text-lg font-mono text-red-400">
               {(data.processing?.deleted_processed || 0).toLocaleString()}
             </div>
            <div className="text-xs text-gray-500">Deleted</div>
          </div>
        </div>
      </div>

      {/* Daily Breakdown Toggle */}
      <div className="mb-4">
        <button
          onClick={() => setShowDailyBreakdown(!showDailyBreakdown)}
          className="flex items-center gap-2 text-sm text-gray-300 hover:text-white transition-colors"
        >
          <BarChart3 className="w-4 h-4" />
          {showDailyBreakdown ? 'Hide' : 'Show'} Daily Breakdown
        </button>
      </div>

      {/* Daily Breakdown Table */}
      {showDailyBreakdown && (
        <div className="bg-gray-700 rounded-lg p-4 mb-4">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-400 border-b border-gray-600">
                  <th className="text-left py-2">Date</th>
                  <th className="text-right py-2">Crawled</th>
                  <th className="text-right py-2">New</th>
                  <th className="text-right py-2">Changed</th>
                  <th className="text-right py-2">Failed</th>
                  <th className="text-right py-2">Deleted</th>
                </tr>
              </thead>
              <tbody>
                                 {(data.daily_breakdown || []).map((day, index) => (
                   <tr key={day.date} className="border-b border-gray-600/50">
                     <td className="py-2 text-gray-300">{formatDate(day.date)}</td>
                     <td className="py-2 text-right font-mono text-blue-300">
                       {(day.stats?.pages_crawled || 0).toLocaleString()}
                     </td>
                     <td className="py-2 text-right font-mono text-green-300">
                       {(day.stats?.new_pages || 0).toLocaleString()}
                     </td>
                     <td className="py-2 text-right font-mono text-yellow-300">
                       {(day.stats?.changed_pages || 0).toLocaleString()}
                     </td>
                     <td className="py-2 text-right font-mono text-red-300">
                       {(day.stats?.failed_pages || 0).toLocaleString()}
                     </td>
                     <td className="py-2 text-right font-mono text-red-400">
                       {(day.stats?.deleted_pages || 0).toLocaleString()}
                     </td>
                   </tr>
                 ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="text-xs text-gray-500 text-center">
        Last 7 days rolling window in {data.timezone} • Auto-refreshes every 10 minutes
      </div>
    </div>
  );
} 