'use client';

import { useState, useEffect } from 'react';
import { Calendar, TrendingUp, Activity, AlertTriangle } from 'lucide-react';

// Helper function to format deleted pages breakdown
function formatDeletedBreakdown(deletions: any): string {
  if (!deletions || deletions.deleted_today === 0) return 'no deletions today';
  
  const parts = [];
  if (deletions.deleted_pdfs > 0) parts.push(`${deletions.deleted_pdfs} PDFs`);
  if (deletions.deleted_documents > 0) parts.push(`${deletions.deleted_documents} docs`);
  if (deletions.deleted_webpages > 0) parts.push(`${deletions.deleted_webpages} pages`);
  if (deletions.deleted_media > 0) parts.push(`${deletions.deleted_media} media`);
  if (deletions.deleted_archives > 0) parts.push(`${deletions.deleted_archives} archives`);
  
  return parts.join(', ') || 'pages deleted';
}

interface DailyStatsData {
  timeframe: string;
  period: string;
  timezone: string;
  discovery: {
    discovered_today: number;
    description: string;
  };
  processing: {
    crawled_today: number;
    new_processed: number;
    changed_processed: number;
    failed_processed: number;
    deleted_processed: number;
    document_processed: number;
    description: string;
  };
  changes: {
    changed_today: number;
    description: string;
  };
  failures: {
    failed_today: number;
    description: string;
  };
  deletions: {
    deleted_today: number;
    deleted_pdfs: number;
    deleted_documents: number;
    deleted_webpages: number;
    deleted_media: number;
    deleted_archives: number;
    description: string;
  };
  current_totals: {
    total_pages: number;
    visited_pages: number;
    remaining_pages: number;
    failed_pages: number;
    deleted_pages: number;
    description: string;
  };
}

interface DailyStatsProps {
  siteId: string;
}

export default function DailyStats({ siteId }: DailyStatsProps) {
  const [data, setData] = useState<DailyStatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (siteId) {
      fetchData();
      
      // Auto-refresh every 5 minutes
      const interval = setInterval(fetchData, 5 * 60 * 1000);
      return () => clearInterval(interval);
    }
  }, [siteId]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(`/api/sites/${siteId}/daily-stats`);
      
      if (!response.ok) {
        throw new Error('Failed to fetch daily stats');
      }
      
      const result = await response.json();
      setData(result);
    } catch (err) {
      console.error('Error fetching daily stats:', err);
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-700 rounded w-40 mb-4"></div>
          <div className="grid grid-cols-2 gap-4">
            <div className="h-20 bg-gray-700 rounded"></div>
            <div className="h-20 bg-gray-700 rounded"></div>
            <div className="h-20 bg-gray-700 rounded"></div>
            <div className="h-20 bg-gray-700 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="text-center text-red-400">
          <AlertTriangle className="w-8 h-8 mx-auto mb-2" />
          <p>Failed to load daily stats</p>
          {error && <p className="text-sm text-gray-500 mt-1">{error}</p>}
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Calendar className="w-5 h-5 text-blue-400" />
        <h3 className="text-lg font-medium text-white">📅 Daily Stats</h3>
        <span className="text-sm text-gray-400">({data.period})</span>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
        {/* Pages Discovered Today */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="w-4 h-4 text-green-400" />
            <span className="text-sm text-gray-300">Discovered</span>
          </div>
          <div className="text-2xl font-mono text-green-300 mb-1">
            {(data.discovery?.discovered_today || 0).toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">pages found today</div>
        </div>

        {/* Pages Crawled Today */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Activity className="w-4 h-4 text-blue-400" />
            <span className="text-sm text-gray-300">Processed</span>
          </div>
          <div className="text-2xl font-mono text-blue-300 mb-1">
            {(data.processing?.crawled_today || 0).toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">pages crawled today</div>
        </div>

        {/* Changes Detected Today */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="w-4 h-4 text-yellow-400" />
            <span className="text-sm text-gray-300">Changed</span>
          </div>
          <div className="text-2xl font-mono text-yellow-300 mb-1">
            {(data.changes?.changed_today || 0).toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">content changes</div>
        </div>

        {/* Failures Today */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle className="w-4 h-4 text-red-400" />
            <span className="text-sm text-gray-300">Failed</span>
          </div>
          <div className="text-2xl font-mono text-red-300 mb-1">
            {(data.failures?.failed_today || 0).toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">load failures</div>
        </div>

        {/* Deletions Today */}
        <div className="bg-gray-700 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle className="w-4 h-4 text-red-500" />
            <span className="text-sm text-gray-300">Deleted</span>
          </div>
          <div className="text-2xl font-mono text-red-400 mb-1">
            {(data.deletions?.deleted_today || 0).toLocaleString()}
          </div>
          <div className="text-xs text-gray-500">
            {formatDeletedBreakdown(data.deletions)}
          </div>
        </div>
      </div>

      {/* Processing Breakdown */}
      <div className="bg-gray-700 rounded-lg p-4">
        <h4 className="text-sm font-medium text-gray-300 mb-3">Processing Breakdown (Today)</h4>
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

      {/* Footer */}
      <div className="mt-4 text-xs text-gray-500 text-center">
        Last 24 hours in {data.timezone} • Auto-refreshes every 5 minutes
      </div>
    </div>
  );
} 