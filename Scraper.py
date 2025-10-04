# -*- coding: utf-8 -*-
"""
Terry White Chemmart Product Scraper with Enhanced Data Cleaning

Scrapes product data from all Terry White Chemmart categories with complete data validation.
Ensures 100% field completeness.
Exports clean CSV data ready for analysis.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import os
import random
import re

# =====================================================================================
# CONFIGURATION SECTION
# =====================================================================================
# This section defines all the constants and settings used throughout the scraper

# Base URL for Terry White Chemmart website
BASE = "https://terrywhitechemmart.com.au"

# API endpoints - Terry White uses a REST API for product data
API_PRODUCT_DETAIL = f"{BASE}/shopping-api/v2/get-product"  # Get detailed product info
API_PRODUCT_LIST = f"{BASE}/shopping-api/v2/get-product-list"  # Get product lists by category

# Request settings for polite scraping
REQUEST_TIMEOUT = 30  # Maximum time to wait for API response (seconds)
DELAY_SECONDS = 1     # Delay between requests to avoid overwhelming the server

# All available product categories on Terry White Chemmart
# These are the main category slugs used in their API
CATEGORIES = [
    "beauty", "cosmetics", "diabetes-ndss", "general-health",
    "gifting-fragrances", "household", "medicines", "mother-baby",
    "personal-care", "skin-care", "vitamins-nutrition", "weight-management"
]

# HTTP headers to mimic a real browser request
# This helps avoid being blocked by anti-bot measures
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*"
}

# =====================================================================================
# DATA CLEANING FUNCTIONS
# =====================================================================================
# These functions handle cleaning and processing of raw scraped data

def clean_text(html_str):
    """Convert HTML to clean plain text.

    This function removes HTML tags and normalizes whitespace from text content.
    Used throughout the scraper to clean product descriptions and other text fields.

    Args:
        html_str (str): Raw HTML string from API response

    Returns:
        str: Clean plain text with normalized whitespace
    """
    # Return empty string if input is None or empty
    if not html_str:
        return ""

    # Use BeautifulSoup to parse HTML and extract text content
    soup = BeautifulSoup(html_str, "html.parser")

    # Get text with space separators, strip whitespace, and normalize multiple spaces
    return " ".join(soup.get_text(separator=" ", strip=True).split())

def clean_ingredients(ingredients_text):
    """
    Enhanced ingredients cleaning function - THE CORE CLEANING FEATURE.

    This function implements the user's requirements:
    1. Converts text to Proper Case (Title Case)
    2. Replaces periods with commas (while preserving decimal numbers)
    3. Cleans up extra whitespace and formatting

    Examples:
        Input:  "AQUA, GLYCERIN, SODIUM LAURYL SULFATE. FRAGRANCE."
        Output: "Aqua, Glycerin, Sodium Lauryl Sulfate, Fragrance"

        Input:  "GLYCERIN (1.5%), WATER. SODIUM CHLORIDE."
        Output: "Glycerin (1.5%), Water, Sodium Chloride"

    Args:
        ingredients_text (str): Raw ingredients text from product data

    Returns:
        str: Cleaned ingredients text in proper format
    """
    # Handle None, empty, or already "N/A" values
    if not ingredients_text or ingredients_text == "N/A":
        return ingredients_text

    # First, clean any HTML tags and normalize whitespace
    cleaned_text = clean_text(ingredients_text)

    # If cleaning resulted in empty text, return "N/A" for consistency
    if not cleaned_text or cleaned_text.strip() == "":
        return "N/A"

    # STEP 1: Replace periods with commas, BUT preserve decimal numbers
    # The regex (?!\d) is a negative lookahead that prevents replacing periods
    # that are followed by digits (decimal numbers like "1.5")
    cleaned_text = re.sub(r'\.(?!\d)', ',', cleaned_text)

    # STEP 2: Convert to proper case (title case)
    # This capitalizes the first letter of each word
    cleaned_text = cleaned_text.title()

    # STEP 3: Fix common English words that shouldn't be capitalized
  
    common_fixes = {
        r'\bAnd\b': 'and',      # Conjunctions
        r'\bOr\b': 'or',
        r'\bOf\b': 'of',        # Prepositions
        r'\bIn\b': 'in',
        r'\bThe\b': 'the',      # Articles
        r'\bWith\b': 'with',
        r'\bFrom\b': 'from',
        r'\bTo\b': 'to',
        r'\bAs\b': 'as',
        r'\bBy\b': 'by',
        r'\bFor\b': 'for',
        r'\bOn\b': 'on',
        r'\bAt\b': 'at',
        r'\bIs\b': 'is',
        r'\bA\b': 'a',
        r'\bAn\b': 'an'
    }

    # Apply each fix using regex word boundaries (\b) to match whole words only
    for pattern, replacement in common_fixes.items():
        cleaned_text = re.sub(pattern, replacement, cleaned_text)

    # STEP 4: Clean up punctuation and spacing issues
    # Remove multiple consecutive commas
    cleaned_text = re.sub(r',\s*,+', ',', cleaned_text)
    # Normalize comma spacing (ensure single space after each comma)
    cleaned_text = re.sub(r'\s*,\s*', ', ', cleaned_text)
    # Normalize all whitespace to single spaces
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    # STEP 5: Remove any trailing commas or spaces
    cleaned_text = cleaned_text.rstrip(', ')

    return cleaned_text

# =====================================================================================
# DATA EXTRACTION FUNCTIONS
# =====================================================================================
# These functions extract specific information from the API response data

def extract_detail(details, label):
    """Extract specific detail content by searching for content_label.

    Terry White's API returns product details as a list of objects, each with
    a 'content_label' and 'content'. This function finds the right detail.

    Args:
        details (list): List of detail objects from API response
        label (str): The content_label to search for (e.g., "Ingredients", "General Information")

    Returns:
        str: Cleaned text content of the found detail, or empty string if not found
    """
    if not details:
        return ""

    # Search through all detail objects to find matching label
    for detail_obj in details:
        if detail_obj.get("content_label") == label:
            # Clean the content to remove HTML and normalize text
            return clean_text(detail_obj.get("content"))

    # Return empty string if label not found
    return ""

def search_text_for_patterns(text, patterns):
    """Search text for any of the given regex patterns.

    This is a utility function that tries multiple regex patterns against text
    and returns the first match found. Used for extracting size/volume information.

    Args:
        text (str): Text to search in
        patterns (list): List of regex pattern strings to try

    Returns:
        re.Match or None: First successful regex match, or None if no patterns match
    """
    if not text:
        return None

    # Try each pattern until we find a match
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)  # Case-insensitive search
        if match:
            return match

    # No patterns matched
    return None

def get_skin_concerns(text):
    """Intelligently derive skin concerns from product descriptions using pattern matching.

    This function analyzes product names and descriptions to automatically categorize
    products by the skin concerns they address. It uses keyword pattern matching
    to identify various skin care categories.

    Args:
        text (str): Product description or name to analyze

    Returns:
        list: List of detected skin concern categories
    """
    if not text:
        return []

    # Convert to lowercase for consistent pattern matching
    text = text.lower()

    # Dictionary mapping skin concerns to their identifying keyword patterns
    # Each concern has multiple patterns to catch different ways of expressing it
    concern_patterns = {
        "acne": [r"acne", r"pimple", r"breakout", r"blemish"],
        "dry skin": [r"dry\s*skin", r"dehydrat", r"moisturiz", r"hydrat"],
        "oily skin": [r"oily\s*skin", r"excess\s*oil", r"sebum"],
        "sensitive skin": [r"sensitive\s*skin", r"gentle", r"sooth", r"calm"],
        "anti-aging": [r"anti.?aging", r"wrinkle", r"fine\s*line", r"firm"],
        "hyperpigmentation": [r"hyperpigment", r"dark\s*spot", r"discolor"],
        "sun protection": [r"spf", r"sun\s*protection", r"sunscreen"],
        "rosacea": [r"rosacea", r"redness"],
        "eczema": [r"eczema", r"dermatitis"],
        "mature skin": [r"mature\s*skin", r"aging\s*skin"],
        "dullness": [r"dull", r"brighten", r"radiance", r"glow"]
    }

    detected_concerns = []

    # Check each concern category
    for concern, patterns in concern_patterns.items():
        # Try each pattern for this concern
        for pattern in patterns:
            if re.search(pattern, text):
                # If pattern matches and concern not already added
                if concern not in detected_concerns:
                    detected_concerns.append(concern)
                break  # Move to next concern once we find a match

    return detected_concerns

def extract_size_volume(product_data):
    """Extract size/volume information from various fields in product data.

    This function searches through multiple product fields to find size/volume
    information using regex patterns. It looks for common units like ml, g, oz, etc.

    Args:
        product_data (dict): Product data dictionary from API

    Returns:
        str: Extracted size/volume (e.g., "50ml", "100g") or "N/A" if not found
    """
    if not product_data:
        return "N/A"

    # Regex patterns to match various size/volume formats
    # Each pattern captures: (number)(optional decimal)(unit)
    size_patterns = [
        r"(\d+(?:\.\d+)?)\s*(ml|mL|ML)",                    # Milliliters
        r"(\d+(?:\.\d+)?)\s*(g|gm|GM|gram|grams)",          # Grams
        r"(\d+(?:\.\d+)?)\s*(oz|OZ|fl\.?\s*oz)",           # Ounces
        r"(\d+(?:\.\d+)?)\s*(l|L|liter|litre)",            # Liters
        r"(\d+)\s*(tablet|tablets|caps|capsule|capsules)",  # Pills/tablets
        r"(\d+)\s*(count|ct|pieces|pcs)",                   # Count/pieces
        r"(\d+(?:\.\d+)?)\s*(kg|KG|kilogram)"              # Kilograms
    ]

    # Search through multiple fields in order of preference
    # Product name is most reliable, then details, then other fields
    search_fields = [
        product_data.get("name", ""),                                           # Product name
        " ".join([d.get("content", "") for d in product_data.get("details", [])]),  # All detail content
        product_data.get("description", ""),                                   # Product description
        str(product_data.get("attributes", {}))                               # Product attributes
    ]

    # Try each field until we find a size match
    for field_content in search_fields:
        if field_content:
            match = search_text_for_patterns(field_content, size_patterns)
            if match:
                # Combine the number and unit (group 1 = number, group 2 = unit)
                return f"{match.group(1)}{match.group(2)}"

    # No size information found
    return "N/A"

def extract_product_line_name(product_data):
    """Extract product line/series name from product data using multiple strategies.

    This function tries to identify the product line or collection name by:
    1. Looking for explicit line information in product details
    2. Parsing the product name structure to identify line names

    Args:
        product_data (dict): Product data dictionary from API

    Returns:
        str: Product line name or "N/A" if not found/determinable
    """
    # STRATEGY 1: Look for explicit line information in product details
    details = product_data.get("details", [])
    for detail in details:
        label = detail.get("content_label", "").lower()
        # Check if this detail contains line/collection information
        if any(keyword in label for keyword in ["line", "collection", "series", "range"]):
            line_name = clean_text(detail.get("content", ""))
            # Ensure it's reasonable length (not a full description)
            if line_name and len(line_name) < 100:
                return line_name

    # STRATEGY 2: Parse product name structure to extract line name
    product_name = product_data.get("name", "")
    brand_name = product_data.get("brand", {}).get("brand_name", "")

    # If we have both product name and brand name
    if brand_name and brand_name in product_name:
        # Remove brand name to get the remaining product identifier
        remaining_name = product_name.replace(brand_name, "").strip()
        name_parts = remaining_name.split()

        # Look for potential line name in the first few words
        if len(name_parts) > 1:
            potential_line_words = []

            # Take capitalized words that likely represent a line name
            for part in name_parts[:3]:  # Check first 3 words only
                # Must be capitalized and reasonable length
                if part and part[0].isupper() and len(part) > 2:
                    potential_line_words.append(part)
                else:
                    break  # Stop at first non-qualifying word

            if potential_line_words:
                line_name = " ".join(potential_line_words)

                # Filter out generic product type terms
                generic_terms = ["cream", "lotion", "serum", "oil", "gel", "cleanser", "moisturiser", "spf", "wash"]

                # Only return if it doesn't seem to be a generic product type
                if not any(term in line_name.lower() for term in generic_terms):
                    return line_name

    # No product line identified
    return "N/A"

def get_product_slugs(category_slug, max_products=10, page_size=24):
    """Fetch product slugs from a specific category."""
    slugs = []
    page = 1
    total_fetched = 0

    request_body = {
        "app_identifier": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "device_identifier": "YmTmalS1e8tKERfvJL8DxElPWlAd24_L5OkwBpF_xek",
        "parameters": {
            "brands": None,
            "categories": [category_slug],
            "conditions": None,
            "use_semantic": True
        },
        "page": page,
        "page_size": page_size
    }

    while total_fetched < max_products:
        request_body["page"] = page

        try:
            resp = requests.post(API_PRODUCT_LIST, headers=HEADERS, json=request_body, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            products = data.get("results", [])
            if not products:
                break

            for prod in products:
                slug = prod.get("slug")
                if slug and slug not in slugs:
                    slugs.append(slug)
                    total_fetched += 1
                    if total_fetched >= max_products:
                        break

            page += 1
            time.sleep(DELAY_SECONDS)

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Error fetching products from {category_slug}: {e}")
            break

    return slugs

def collect_random_slugs(categories, total_products=10):
    """Get randomized product slugs from multiple categories."""
    all_slugs = []
    products_per_category = max(1, total_products // len(categories))

    for category in categories:
        category_slugs = get_product_slugs(category, max_products=products_per_category + 5)
        all_slugs.extend(category_slugs)

    # Remove duplicates and randomize
    unique_slugs = list(dict.fromkeys(all_slugs))
    if len(unique_slugs) > total_products:
        random.shuffle(unique_slugs)
        unique_slugs = unique_slugs[:total_products]

    return unique_slugs

def fetch_product_data(slug):
    """Fetch detailed product information from API."""
    body = {"product_slug": slug, "extensions": {}}

    try:
        resp = requests.post(API_PRODUCT_DETAIL, headers=HEADERS, json=body, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data.get("product")
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"Error fetching product {slug}: {e}")
        return None

def validate_completeness(row):
    """Validate product has complete data (except Product Line Name can be N/A)."""
    required_fields = [
        "Product ID", "Product Name", "Brand Name", "Product Description",
        "Product Images", "Barcode (EAN/UPC)", "Price", "Size/Volume",
        "Ingredients", "Skin Concern", "Source URL"
    ]

    for field in required_fields:
        if row.get(field) == "N/A" or not row.get(field):
            return False, field

    return True, None

def scrape_products(categories=None, max_products=10, verbose=False):
    """
    Main scraping function with data completeness validation.

    Args:
        categories (list): Categories to scrape (default: all categories)
        max_products (int): Number of complete products to find
        verbose (bool): Enable detailed logging

    Returns:
        list: Complete product data
    """
    if categories is None:
        categories = CATEGORIES

    if verbose:
        print(f"Scraping {max_products} complete products from {len(categories)} categories")

    rows = []
    candidate_slugs_queue = []
    attempts = 0
    max_attempts = max_products * 5

    while len(rows) < max_products and attempts < max_attempts:
        # Refill candidate queue if empty
        if not candidate_slugs_queue:
            batch_size = min(50, max_attempts - attempts + 10)
            new_slugs = collect_random_slugs(categories, batch_size)
            candidate_slugs_queue.extend([slug for slug in new_slugs if slug not in candidate_slugs_queue])

        if not candidate_slugs_queue:
            break

        slug = candidate_slugs_queue.pop(0)
        attempts += 1

        if verbose:
            print(f"[{attempts}] Testing: {slug} (found {len(rows)}/{max_products})")

        # Fetch and validate product
        prod = fetch_product_data(slug)
        if prod is None:
            continue

        # Extract all data fields
        brand_name = prod.get("brand", {}).get("brand_name", "N/A")
        image_urls = list(prod.get("images", {}).values())
        general_info = extract_detail(prod.get("details"), "General Information")
        ingredients = extract_detail(prod.get("details"), "Ingredients")

        # Apply enhanced ingredients cleaning
        cleaned_ingredients = clean_ingredients(ingredients)

        combined_text = f"{general_info} {prod.get('name', '')}"
        skin_concerns = get_skin_concerns(combined_text)
        size_volume = extract_size_volume(prod)
        product_line_name = extract_product_line_name(prod)

        row = {
            "Product ID": prod.get("product_id", "N/A"),
            "Product Name": prod.get("name", "N/A"),
            "Product Line Name": product_line_name,
            "Brand Name": brand_name,
            "Product Description": general_info if general_info else "N/A",
            "Product Images": "|".join(image_urls) if image_urls else "N/A",
            "Barcode (EAN/UPC)": prod.get("upc", "N/A"),
            "Price": str(prod.get("price", "N/A")),
            "Size/Volume": size_volume,
            "Ingredients": cleaned_ingredients if cleaned_ingredients else "N/A",
            "Skin Concern": ", ".join(skin_concerns) if skin_concerns else "N/A",
            "Source URL": f"{BASE}/shop/product/{slug}"
        }

        # Validate completeness
        is_complete, missing_field = validate_completeness(row)

        if is_complete:
            rows.append(row)
            if verbose:
                print(f"COMPLETE: {prod.get('name', 'Unknown')}")
        elif verbose:
            print(f"❌ INCOMPLETE: Missing '{missing_field}'")

        time.sleep(DELAY_SECONDS)

    print(f"Found {len(rows)} complete products after {attempts} attempts")
    return rows

def save_to_csv(rows, filename="terry_white_products_cleaned.csv"):
    """Save scraped data to CSV with summary statistics."""
    if not rows:
        print("No data to save.")
        return None

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, encoding="utf-8")

    print(f"Saved {len(rows)} products to {filename}")
    print(f"Unique brands: {df['Brand Name'].nunique()}")

    # Field completeness analysis
    print("\nField Completeness:")
    for col in df.columns:
        non_na_count = sum(1 for x in df[col] if x != 'N/A')
        completeness = (non_na_count / len(df)) * 100
        status = "OK" if completeness == 100 else "!" if completeness >= 80 else "!!!"
        print(f"{status} {col}: {completeness:.1f}%")

    print(f"\nIngredients cleaning applied: Proper case formatting + period-to-comma conversion")

    return df

# =====================================================================================
# MAIN EXECUTION BLOCK
# =====================================================================================
# This runs when the script is executed directly (not imported)

if __name__ == "__main__":
    print("=== PRODUCT SCRAPER WITH ENHANCED CLEANING ===")
    print("Features: cleaning")
    print("Quality: 100% field completeness validation\n")

    # CONFIGURATION SETTINGS
    MAX_PRODUCTS = 10    # Number of complete products to collect
    VERBOSE = True       # Set to False for quiet operation

    # EXECUTE MAIN SCRAPING PROCESS
    print("Starting scraping process...")
    data = scrape_products(
        categories=CATEGORIES,     # Scrape from all available categories
        max_products=MAX_PRODUCTS, # Target number of complete products
        verbose=VERBOSE            # Enable detailed logging
    )

    # SAVE RESULTS TO CSV
    print("\nSaving results to CSV...")
    df = save_to_csv(data)

    # FINAL SUCCESS REPORT
    if df is not None:
        print(f"\n🎉 SCRAPING COMPLETED SUCCESSFULLY!")
        print(f"File saved to: {os.path.abspath('Product_data.csv')}")
        print(f"Dataset: {len(df)} products with enhanced cleaned ingredients")
        print(f"Ready for Task 2 analysis using Grouping.py")
    else:
        print("\nNo data was collected. Check your internet connection and try again.")