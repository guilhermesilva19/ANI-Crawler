'use client';

import { useState } from 'react';
import SiteSelector from '@/components/SiteSelector';
import URLBrowser from '@/components/URLBrowser';

export default function URLsPage() {
  const [selectedSite, setSelectedSite] = useState<string | null>(null);

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      {/* Header */}
      <header className="border-b border-gray-700 bg-gray-800">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-6">
              <h1 className="text-xl font-bold text-white">ANI-Crawler Dashboard</h1>
              <nav className="flex items-center gap-4">
                <a
                  href="/"
                  className="px-3 py-1 bg-gray-700 text-gray-300 rounded-lg hover:bg-gray-600 transition-colors text-sm"
                >
                  Dashboard
                </a>
                <a
                  href="/urls"
                  className="px-3 py-1 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm"
                >
                  URL Browser
                </a>
              </nav>
              <div className="text-sm text-gray-400">
                URL Management
              </div>
            </div>
            <SiteSelector 
              selectedSite={selectedSite} 
              onSiteChange={setSelectedSite}
            />
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">

        {selectedSite ? (
          <URLBrowser siteId={selectedSite} />
        ) : (
          <div className="text-center py-16">
            <div className="text-gray-400 text-lg mb-4">
              Select a site to view URLs
            </div>
            <div className="text-gray-500 text-sm">
              Choose a site from the dropdown in the header to browse URLs
            </div>
          </div>
        )}
      </main>
    </div>
  );
} 