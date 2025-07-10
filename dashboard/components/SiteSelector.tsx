'use client';

import { useState, useEffect } from 'react';
import { ChevronDown, Database, Clock } from 'lucide-react';

interface Site {
  siteId: string;
  totalPages: number;
  currentCycle: number;
  isFirstCycle: boolean;
  lastUpdated?: string;
}

interface SiteSelectorProps {
  selectedSite: string | null;
  onSiteChange: (siteId: string) => void;
}

export default function SiteSelector({ selectedSite, onSiteChange }: SiteSelectorProps) {
  const [sites, setSites] = useState<Site[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSites();
  }, []);

  const fetchSites = async () => {
    try {
      const response = await fetch('/api/sites');
      const data = await response.json();
      setSites(data.sites || []);
      
      // Auto-select first site if none selected
      if (!selectedSite && data.sites?.length > 0) {
        onSiteChange(data.sites[0].siteId);
      }
    } catch (error) {
      console.error('Error fetching sites:', error);
    } finally {
      setLoading(false);
    }
  };

  const selectedSiteData = sites.find(s => s.siteId === selectedSite);

  if (loading) {
    return (
      <div className="flex items-center gap-2 px-4 py-2 bg-gray-800 rounded-lg">
        <Database className="w-4 h-4 text-gray-400" />
        <span className="text-gray-400">Loading sites...</span>
      </div>
    );
  }

  if (sites.length === 0) {
    return (
      <div className="flex items-center gap-2 px-4 py-2 bg-gray-800 rounded-lg">
        <Database className="w-4 h-4 text-gray-400" />
        <span className="text-gray-400">No sites found</span>
      </div>
    );
  }

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-3 px-4 py-2 bg-gray-800 hover:bg-gray-700 rounded-lg transition-colors min-w-64"
      >
        <Database className="w-4 h-4 text-blue-400" />
        <div className="flex-1 text-left">
          <div className="text-white font-medium">
            {selectedSiteData?.siteId || 'Select Site'}
          </div>
          {selectedSiteData && (
            <div className="text-xs text-gray-400">
              Cycle {selectedSiteData.currentCycle} • {selectedSiteData.totalPages.toLocaleString()} pages
            </div>
          )}
        </div>
        <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 mt-1 w-full bg-gray-800 rounded-lg shadow-lg border border-gray-700 z-50">
          {sites.map((site) => (
            <button
              key={site.siteId}
              onClick={() => {
                onSiteChange(site.siteId);
                setIsOpen(false);
              }}
              className={`w-full px-4 py-3 text-left hover:bg-gray-700 transition-colors first:rounded-t-lg last:rounded-b-lg ${
                selectedSite === site.siteId ? 'bg-gray-700' : ''
              }`}
            >
              <div className="flex items-center gap-3">
                <Database className="w-4 h-4 text-blue-400" />
                <div className="flex-1">
                  <div className="text-white font-medium">{site.siteId}</div>
                  <div className="text-xs text-gray-400 flex items-center gap-4">
                    <span>Cycle {site.currentCycle} ({site.isFirstCycle ? 'Discovery' : 'Maintenance'})</span>
                    <span>{site.totalPages.toLocaleString()} pages</span>
                    {site.lastUpdated && (
                      <span className="flex items-center gap-1">
                        <Clock className="w-3 h-3" />
                        {new Date(site.lastUpdated).toLocaleDateString()}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
} 