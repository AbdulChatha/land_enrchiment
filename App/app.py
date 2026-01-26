"""
Flask Backend - Land Enrichment App
Direct API Integration with Land.com and LandWatch
Real-time scraping, no land database
"""

from flask import Flask, render_template, jsonify, request
import sqlite3
import time
from curl_cffi import requests
from math import radians, cos, sin, asin, sqrt

app = Flask(__name__)
DB_PATH = 'builders.db'

# State code to full name mapping (for LandWatch API)
STATE_CODE_TO_NAME = {
    'AL': 'alabama', 'AZ': 'arizona', 'AR': 'arkansas', 'CA': 'california',
    'CO': 'colorado', 'DE': 'delaware', 'FL': 'florida', 'GA': 'georgia',
    'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa',
    'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana', 'MD': 'maryland',
    'MN': 'minnesota', 'MS': 'mississippi', 'MO': 'missouri', 'NE': 'nebraska',
    'NV': 'nevada', 'NH': 'new-hampshire', 'NJ': 'new-jersey', 'NM': 'new-mexico',
    'NY': 'new-york', 'NC': 'north-carolina', 'OH': 'ohio', 'OK': 'oklahoma',
    'OR': 'oregon', 'PA': 'pennsylvania', 'SC': 'south-carolina', 'TN': 'tennessee',
    'TX': 'texas', 'UT': 'utah', 'VA': 'virginia', 'WA': 'washington',
    'WV': 'west-virginia', 'WI': 'wisconsin', 'DC': 'washington-dc'
}

# Property type mappings
LANDCOM_PROPERTY_TYPES = {
    'homesite': 8,
    'recreational': 4,
    'waterfront': 3584,
    'undeveloped': 32,
    'commercial': 64
}

LANDWATCH_PROPERTY_TYPES = {
    'recreational': 4,
    'undeveloped': 32,
    'commercial': 64,
    'homesite': 4096,
    'waterfront': 3584
}


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Returns distance in miles
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in miles
    miles = 3956 * c
    return round(miles, 2)


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================================
# LAND.COM SCRAPER
# ============================================================================

def scrape_land_com(city, state_code, min_price=0, max_price=1000000, min_acres=0, max_acres=100, property_types=None):
    """
    Scrape Land.com API with pagination
    URL: /{city}-{state}/all-land/no-house/for-sale/{price}/{acres}/is-active/type-{number}/
    """
    print(f"\n🌍 Scraping Land.com: {city}, {state_code}")
    
    # Calculate property type filter
    type_filter = 3692  # Default (all types)
    if property_types:
        type_sum = sum(LANDCOM_PROPERTY_TYPES.get(pt, 0) for pt in property_types)
        if type_sum > 0:
            type_filter = type_sum
    
    # Build URL components
    city_slug = city.lower().replace(' ', '-')
    price_filter = f"under-{max_price}"
    acres_filter = f"under-{max_acres}-acres" if max_acres < 1000 else "any-size"
    
    base_url = f"https://www.land.com/api/property/search/0/{city_slug}-{state_code}/all-land/no-house/for-sale/{price_filter}/{acres_filter}/is-active/type-{type_filter}/"
    
    print(f"   Property type filter: {type_filter}")
    
    all_listings = []
    
    try:
        # Create session
        session = requests.Session()
        session.get("https://www.land.com", impersonate="chrome")
        time.sleep(0.5)
        
        # Fetch first page to get total count
        response = session.get(base_url, impersonate="chrome", timeout=30)
        response.raise_for_status()
        first_page_data = response.json()
        
        # Get total count and calculate pages
        search_results = first_page_data.get('searchResults', {})
        total_count = search_results.get('totalCount', 0)
        properties = search_results.get('propertyResults', [])
        listings_per_page = len(properties)
        
        if listings_per_page > 0:
            total_pages = min((total_count + listings_per_page - 1) // listings_per_page, 20)  # Max 20 pages
            print(f"   Total: {total_count} listings, fetching {total_pages} pages")
        else:
            print(f"   No listings found")
            return []
        
        # Process first page
        for prop in properties:
            listing = {
                'listing_id': f"landcom_{prop.get('siteListingId', '')}",
                'source': 'Land.com',
                'title': prop.get('title', ''),
                'city': prop.get('city', ''),
                'state': prop.get('stateAbbreviation', ''),
                'zip': prop.get('zip', ''),
                'county': prop.get('county', ''),
                'latitude': prop.get('latitude'),
                'longitude': prop.get('longitude'),
                'acres': prop.get('acres'),
                'price': prop.get('price'),
                'description': prop.get('description', ''),
                'listing_url': f"https://www.land.com{prop.get('canonicalUrl', '')}"
            }
            
            # Filter by price and acres
            if listing['latitude'] and listing['longitude']:
                if listing['price'] and listing['acres']:
                    if min_price <= listing['price'] <= max_price and min_acres <= listing['acres'] <= max_acres:
                        all_listings.append(listing)
                elif listing['acres'] and min_acres <= listing['acres'] <= max_acres:
                    all_listings.append(listing)
        
        # Fetch remaining pages
        for page_num in range(2, total_pages + 1):
            time.sleep(1)  # Be polite
            
            page_url = f"{base_url}page-{page_num}/"
            response = session.get(page_url, impersonate="chrome", timeout=30)
            
            if response.status_code != 200:
                continue
                
            page_data = response.json()
            properties = page_data.get('searchResults', {}).get('propertyResults', [])
            
            for prop in properties:
                listing = {
                    'listing_id': f"landcom_{prop.get('siteListingId', '')}",
                    'source': 'Land.com',
                    'title': prop.get('title', ''),
                    'city': prop.get('city', ''),
                    'state': prop.get('stateAbbreviation', ''),
                    'zip': prop.get('zip', ''),
                    'county': prop.get('county', ''),
                    'latitude': prop.get('latitude'),
                    'longitude': prop.get('longitude'),
                    'acres': prop.get('acres'),
                    'price': prop.get('price'),
                    'description': prop.get('description', ''),
                    'listing_url': f"https://www.land.com{prop.get('canonicalUrl', '')}"
                }
                
                if listing['latitude'] and listing['longitude']:
                    if listing['price'] and listing['acres']:
                        if min_price <= listing['price'] <= max_price and min_acres <= listing['acres'] <= max_acres:
                            all_listings.append(listing)
                    elif listing['acres'] and min_acres <= listing['acres'] <= max_acres:
                        all_listings.append(listing)
            
            print(f"   Page {page_num}/{total_pages}: {len(all_listings)} total listings")
        
        print(f"   ✅ Land.com: {len(all_listings)} listings")
        
    except Exception as e:
        print(f"   ❌ Land.com error: {e}")
    
    return all_listings


# ============================================================================
# LANDWATCH SCRAPER
# ============================================================================

def scrape_landwatch(city, state_code, min_price=0, max_price=1000000, min_acres=0, max_acres=100, property_types=None):
    """
    Scrape LandWatch API with pagination
    URL: /{state}-land-for-sale/{city}/prop-types-{number}/price-{min}-{max}/acres-{min}-{max}/available
    """
    print(f"\n🌍 Scraping LandWatch: {city}, {state_code}")
    
    # Calculate property type filter
    type_filter = 7780  # Default (all types)
    if property_types:
        type_sum = sum(LANDWATCH_PROPERTY_TYPES.get(pt, 0) for pt in property_types)
        if type_sum > 0:
            type_filter = type_sum
    
    # Convert state code to name
    state_name = STATE_CODE_TO_NAME.get(state_code, state_code.lower())
    city_slug = city.lower().replace(' ', '-')
    
    # Build URL
    base_url = f"https://www.landwatch.com/api/property/search/1113/{state_name}-land-for-sale/{city_slug}/prop-types-{type_filter}/price-{min_price}-{max_price}/acres-{min_acres}-{max_acres}/available"
    
    print(f"   Property type filter: {type_filter}")
    
    all_listings = []
    
    try:
        # Fetch first page to get total count
        response = requests.get(
            base_url,
            impersonate="chrome",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.landwatch.com/",
            },
            timeout=30
        )
        
        response.raise_for_status()
        first_page_data = response.json()
        
        # Get total count and calculate pages
        search_results = first_page_data.get('searchResults', {})
        total_count = search_results.get('totalCount', 0)
        properties = search_results.get('propertyResults', [])
        listings_per_page = len(properties)
        
        if listings_per_page > 0:
            total_pages = min((total_count + listings_per_page - 1) // listings_per_page, 20)  # Max 20 pages
            print(f"   Total: {total_count} listings, fetching {total_pages} pages")
        else:
            print(f"   No listings found")
            return []
        
        # Process first page
        for prop in properties:
            listing = {
                'listing_id': f"landwatch_{prop.get('lwPropertyId', '')}",
                'source': 'LandWatch',
                'title': prop.get('title', ''),
                'city': prop.get('city', ''),
                'state': prop.get('stateAbbreviation', ''),
                'zip': prop.get('zip', ''),
                'county': prop.get('county', ''),
                'latitude': prop.get('latitude'),
                'longitude': prop.get('longitude'),
                'acres': prop.get('acres'),
                'price': prop.get('price'),
                'description': prop.get('description', ''),
                'listing_url': f"https://www.landwatch.com{prop.get('canonicalUrl', '')}"
            }
            
            # Only include if has coordinates
            if listing['latitude'] and listing['longitude']:
                all_listings.append(listing)
        
        # Fetch remaining pages
        for page_num in range(2, total_pages + 1):
            time.sleep(1)  # Be polite
            
            page_url = f"{base_url}/page-{page_num}"
            response = requests.get(
                page_url,
                impersonate="chrome",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.landwatch.com/",
                },
                timeout=30
            )
            
            if response.status_code != 200:
                continue
            
            page_data = response.json()
            properties = page_data.get('searchResults', {}).get('propertyResults', [])
            
            for prop in properties:
                listing = {
                    'listing_id': f"landwatch_{prop.get('lwPropertyId', '')}",
                    'source': 'LandWatch',
                    'title': prop.get('title', ''),
                    'city': prop.get('city', ''),
                    'state': prop.get('stateAbbreviation', ''),
                    'zip': prop.get('zip', ''),
                    'county': prop.get('county', ''),
                    'latitude': prop.get('latitude'),
                    'longitude': prop.get('longitude'),
                    'acres': prop.get('acres'),
                    'price': prop.get('price'),
                    'description': prop.get('description', ''),
                    'listing_url': f"https://www.landwatch.com{prop.get('canonicalUrl', '')}"
                }
                
                if listing['latitude'] and listing['longitude']:
                    all_listings.append(listing)
            
            print(f"   Page {page_num}/{total_pages}: {len(all_listings)} total listings")
        
        print(f"   ✅ LandWatch: {len(all_listings)} listings")
        
    except Exception as e:
        print(f"   ❌ LandWatch error: {e}")
    
    return all_listings


# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Serve main page"""
    return render_template('index.html')


@app.route('/api/cities')
def get_cities():
    """Get list of cities with builders (single selection dropdown)"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get cities where we have builders (so we can show builders on map)
    cursor.execute("""
        SELECT DISTINCT
            id,
            city,
            state,
            latitude,
            longitude,
            city_rating,
            rating_category,
            community_count,
            builder_count
        FROM city_ratings
        WHERE city_rating >= 30
        ORDER BY city_rating DESC
    """)
    
    cities = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(cities)


@app.route('/api/listings')
def get_listings():
    """
    Get land listings from APIs in real-time
    Scrapes Land.com and LandWatch based on filters
    """
    # Get parameters
    city_id = request.args.get('city_id', type=int)
    min_price = request.args.get('min_price', 0, type=int)
    max_price = request.args.get('max_price', 1000000, type=int)
    min_acres = request.args.get('min_acres', 0, type=int)
    max_acres = request.args.get('max_acres', 100, type=int)
    sources = request.args.getlist('sources')  # ['landcom', 'landwatch']
    property_types = request.args.getlist('property_types')  # ['homesite', 'recreational', etc.]
    
    if not city_id:
        return jsonify({'error': 'City ID required'}), 400
    
    # Get city details from database
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT city, state, latitude, longitude FROM city_ratings WHERE id = ?", (city_id,))
    city_row = cursor.fetchone()
    conn.close()
    
    if not city_row:
        return jsonify({'error': 'City not found'}), 404
    
    city = city_row['city']
    state = city_row['state']
    city_lat = city_row['latitude']
    city_lon = city_row['longitude']
    
    print(f"\n{'='*60}")
    print(f"🔍 Searching land in {city}, {state}")
    print(f"   Price: ${min_price:,} - ${max_price:,}")
    print(f"   Acres: {min_acres} - {max_acres}")
    print(f"   Sources: {sources if sources else 'All'}")
    print(f"   Property Types: {property_types if property_types else 'All'}")
    print(f"{'='*60}")
    
    # Scrape selected sources (default to both)
    all_listings = []
    
    if not sources or 'landcom' in sources:
        landcom_listings = scrape_land_com(city, state, min_price, max_price, min_acres, max_acres, property_types)
        all_listings.extend(landcom_listings)
    
    if not sources or 'landwatch' in sources:
        landwatch_listings = scrape_landwatch(city, state, min_price, max_price, min_acres, max_acres, property_types)
        all_listings.extend(landwatch_listings)
    
    # Calculate distance to city center for each listing
    if city_lat and city_lon:
        for listing in all_listings:
            if listing['latitude'] and listing['longitude']:
                distance = haversine(city_lon, city_lat, listing['longitude'], listing['latitude'])
                listing['distance_to_center'] = distance
            else:
                listing['distance_to_center'] = None
    else:
        for listing in all_listings:
            listing['distance_to_center'] = None
    
    print(f"\n📊 Total listings found: {len(all_listings)}")
    print(f"{'='*60}\n")
    
    return jsonify(all_listings)


@app.route('/api/builders')
def get_builders():
    """Get builders in selected city"""
    city_id = request.args.get('city_id', type=int)
    
    if not city_id:
        return jsonify([])
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Get city name and state
    cursor.execute("SELECT city, state FROM city_ratings WHERE id = ?", (city_id,))
    city_row = cursor.fetchone()
    
    if not city_row:
        conn.close()
        return jsonify([])
    
    # Get builders in that city
    cursor.execute("""
        SELECT 
            id, builder_name, project_name, city, state,
            latitude, longitude
        FROM builders
        WHERE city = ? AND state = ?
    """, (city_row['city'], city_row['state']))
    
    builders = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(builders)


@app.route('/api/stats')
def get_stats():
    """Get database statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM city_ratings")
    total_cities = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM builders")
    total_builders = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT state) FROM builders")
    builder_states = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "cities": {"total": total_cities},
        "builders": {"total": total_builders, "states": builder_states},
        "note": "Land data fetched in real-time from APIs"
    })


if __name__ == '__main__':
    print("="*60)
    print("🏞️  Land Enrichment App Starting...")
    print("="*60)
    print("\n🌐 Server: http://localhost:8080")
    print("📊 Database: builders.db")
    print("🌍 Live APIs: Land.com + LandWatch")
    print("\n✅ Open your browser to http://localhost:8080\n")
    
    app.run(debug=True, host='0.0.0.0', port=8080)