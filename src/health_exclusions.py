"""Health domain exclusions and splitting configuration.

This file contains specific exclusions for the health.gov.au domain
to reduce its size and improve crawling efficiency.
"""

# Health domain patterns to exclude (large, frequently changing, or low-value content)
HEALTH_EXCLUSIONS = [
    # News and media (frequently changing, covered by other monitoring)
    "https://www.health.gov.au/news",
    "https://www.health.gov.au/media",
    "https://www.health.gov.au/newsroom", 
    "https://www.health.gov.au/ministers",
    
    # Event and calendar pages (high churn, low content value)
    "https://www.health.gov.au/events",
    "https://www.health.gov.au/calendar",
    
    # Archive sections (historical content, rarely changes)
    "https://www.health.gov.au/internet/main/publishing.nsf/Content/archive",
    "https://www.health.gov.au/internet/archive",
    
    # Large download sections (should be monitored for availability, not content)
    "https://www.health.gov.au/resources/downloads/all",
    "https://www.health.gov.au/resources/publications/all",
    
    # Administrative and technical pages
    "https://www.health.gov.au/sitemap",
    "https://www.health.gov.au/accessibility",
    "https://www.health.gov.au/copyright",
    "https://www.health.gov.au/privacy",
    "https://www.health.gov.au/disclaimer",
    
    # COVID-19 legacy content (mostly static now)
    "https://www.health.gov.au/news/health-alerts/novel-coronavirus-2019-ncov-health-alert/coronavirus-covid-19-current-situation-and-case-numbers",
    "https://www.health.gov.au/news/health-alerts/novel-coronavirus-2019-ncov-health-alert/coronavirus-covid-19-advice-for-international-travellers",
    
    # Large datasets and statistical reports (monitor for availability, not content changes)
    "https://www.health.gov.au/resources/collections",
    "https://www.health.gov.au/about-us/corporate-reporting/annual-reports",
    
    # Consultation and engagement archives (completed consultations)
    "https://consultations.health.gov.au/closed",
    
    # Very technical/developer content
    "https://www.health.gov.au/internet/main/publishing.nsf/Content/health-xml",
    "https://www.health.gov.au/internet/main/publishing.nsf/Content/api",
]

# High priority health URLs that should be crawled frequently
HIGH_PRIORITY_HEALTH_URLS = [
    "https://www.health.gov.au/",
    "https://www.health.gov.au/health-alerts",
    "https://www.health.gov.au/health-topics",
    "https://www.health.gov.au/our-work",
    "https://www.health.gov.au/ministers/the-hon-mark-butler-mp",
    "https://www.health.gov.au/about-us/contact-us",
    "https://www.health.gov.au/resources",
    "https://www.health.gov.au/initiatives-and-programs",
]

# Medium priority sections (important but less time-sensitive)
MEDIUM_PRIORITY_HEALTH_URLS = [
    "https://www.health.gov.au/topics/medicare",
    "https://www.health.gov.au/topics/mental-health",
    "https://www.health.gov.au/topics/immunisation",
    "https://www.health.gov.au/topics/aged-care",
    "https://www.health.gov.au/topics/chronic-conditions",
    "https://www.health.gov.au/topics/preventive-health",
]

def should_exclude_health_url(url: str) -> bool:
    """Check if a health.gov.au URL should be excluded from crawling."""
    url_lower = url.lower()
    
    # Check against exclusion patterns
    for exclusion in HEALTH_EXCLUSIONS:
        if url_lower.startswith(exclusion.lower()):
            return True
    
    # Exclude very old content (before 2020 for health due to COVID policy changes)
    import re
    year_match = re.search(r'/(\d{4})/', url)
    if year_match:
        year = int(year_match.group(1))
        if year < 2020:
            return True
    
    return False

def get_health_url_priority(url: str) -> int:
    """Get priority level for health.gov.au URLs (1=highest, 3=lowest)."""
    url_lower = url.lower()
    
    # Check high priority URLs
    for priority_url in HIGH_PRIORITY_HEALTH_URLS:
        if url_lower.startswith(priority_url.lower()):
            return 1
    
    # Check medium priority URLs  
    for priority_url in MEDIUM_PRIORITY_HEALTH_URLS:
        if url_lower.startswith(priority_url.lower()):
            return 2
    
    # Default to lower priority
    return 3
