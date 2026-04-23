import requests
import re
import json
import time
from typing import List, Dict, Optional
from datetime import datetime


class ComprehensiveVeliScraper:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.all_products = []
        self.processed_categories = set()
        self.category_tree = []

    def fetch_all_categories(self) -> List[Dict]:
        url = "https://veli.store/api/product/category/"
        print("📁 Fetching all categories from API...")

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            categories = data.get('results', [])
            print(f"✓ Found {len(categories)} top-level categories")
            return categories
        except Exception as e:
            print(f"✗ Error fetching categories: {e}")
            return []

    def fetch_category_details(self, category_id: int) -> Optional[Dict]:
        url = f"https://veli.store/api/product/category/{category_id}/"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"  ✗ Error fetching category {category_id}: {e}")
            return None

    def extract_products_from_html(self, html_content: str) -> List[Dict]:
        products = []
        sku_pattern = r'href="(/details/[^"?]+)\?sku=([^"&]+)"'
        sku_matches = list(re.finditer(sku_pattern, html_content))

        processed_skus = set()

        for match in sku_matches:
            product_slug = match.group(1)
            sku = match.group(2)

            if sku in processed_skus:
                continue
            processed_skus.add(sku)

            start = max(0, match.start() - 500)
            end = min(len(html_content), match.end() + 1500)
            product_html = html_content[start:end]

            current_price_match = re.search(r'<span class="price">(\d+(?:\.\d{2})?)', product_html)
            if not current_price_match:
                continue

            product = {
                'sku': sku,
                'url': f"https://veli.store{product_slug}?sku={sku}",
                'current_price': float(current_price_match.group(1)),
                'currency': 'GEL'
            }

            old_price_match = re.search(r'<span class="old-price">(\d+(?:\.\d{2})?)', product_html)
            if old_price_match:
                product['original_price'] = float(old_price_match.group(1))
                product['savings'] = round(product['original_price'] - product['current_price'], 2)

            discount_match = re.search(r'<span class="sale">-<!--\s*-->(\d+)<!--\s*-->%', product_html)
            if discount_match:
                product['discount_percent'] = int(discount_match.group(1))
            elif 'original_price' in product:
                discount = ((product['original_price'] - product['current_price'])
                           / product['original_price'] * 100)
                product['discount_percent'] = round(discount, 1)

            title_match = re.search(r'<a class="product-title-link"[^>]*>([^<]+)</a>', product_html)
            if title_match:
                product['name'] = title_match.group(1).strip()
            else:
                alt_match = re.search(r'alt="([^"]+)"', product_html)
                if alt_match:
                    product['name'] = alt_match.group(1).strip()

            products.append(product)

        return products

    def scrape_category_page(self, category_slug: str, category_id: int, page: int = 1) -> tuple:
        if page == 1:
            url = f"https://veli.store/category/{category_slug}/{category_id}/"
        else:
            url = f"https://veli.store/category/{category_slug}/{category_id}/?page={page}"

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            html_content = response.text

            products = self.extract_products_from_html(html_content)

            next_page_pattern = rf'href="[^"]*\?[^"]*page={page + 1}[^"]*"'
            has_next = bool(re.search(next_page_pattern, html_content))

            if not has_next and len(products) >= 40:
                has_next = True

            return products, has_next

        except Exception as e:
            print(f"    ✗ Error on page {page}: {e}")
            return [], False

    def scrape_all_pages(self, category_name: str, category_slug: str, category_id: int) -> List[Dict]:
        all_products = []
        page = 1
        max_pages = 50

        while page <= max_pages:
            products, has_next = self.scrape_category_page(category_slug, category_id, page)

            if not products:
                if page == 1:
                    print(f"    ⚠ No products found")
                break

            all_products.extend(products)
            print(f"    Page {page}: {len(products)} products")

            if not has_next:
                break

            page += 1
            time.sleep(0.5)

        for product in all_products:
            product['category_name'] = category_name
            product['category_id'] = category_id

        return all_products

    def process_category(self, category: Dict, depth: int = 0) -> None:
        indent = "  " * depth
        category_id = category.get('id')
        category_name = category.get('headline', 'Unknown')
        category_slug = category.get('full_slug', '')
        has_subcategory = category.get('has_subcategory', False)

        if category_id in self.processed_categories:
            return

        self.processed_categories.add(category_id)

        print(f"\n{indent}📂 {category_name} (ID: {category_id})")

        if has_subcategory:
            print(f"{indent}   ↳ Has subcategories, fetching...")
            details = self.fetch_category_details(category_id)

            if details and 'sub_category' in details:
                subcategories = details['sub_category']
                print(f"{indent}   ✓ Found {len(subcategories)} subcategories")

                self.category_tree.append({
                    'id': category_id,
                    'name': category_name,
                    'depth': depth,
                    'has_subcategories': True,
                    'subcategory_count': len(subcategories)
                })

                for subcat in subcategories:
                    time.sleep(0.3)
                    self.process_category(subcat, depth + 1)
            else:
                print(f"{indent}   ⚠ Could not fetch subcategories, scraping products...")
                products = self.scrape_all_pages(category_name, category_slug, category_id)

                if products:
                    print(f"{indent}   ✓ Scraped {len(products)} products")
                    self.all_products.extend(products)
                    self.category_tree.append({
                        'id': category_id,
                        'name': category_name,
                        'depth': depth,
                        'has_subcategories': False,
                        'product_count': len(products)
                    })
        else:
            print(f"{indent}   ↳ Leaf category, scraping products...")
            products = self.scrape_all_pages(category_name, category_slug, category_id)

            if products:
                print(f"{indent}   ✓ Scraped {len(products)} products")
                self.all_products.extend(products)

            self.category_tree.append({
                'id': category_id,
                'name': category_name,
                'depth': depth,
                'has_subcategories': False,
                'product_count': len(products)
            })

    def scrape_everything(self) -> Dict:
        print("=" * 70)
        print("COMPREHENSIVE VELI STORE SCRAPER")
        print("=" * 70)
        print()

        start_time = time.time()

        categories = self.fetch_all_categories()

        if not categories:
            print("No categories found!")
            return {'products': [], 'categories': []}

        print(f"\n{'=' * 70}")
        print("PROCESSING CATEGORY TREE")
        print(f"{'=' * 70}")

        for i, category in enumerate(categories, 1):
            print(f"\n[{i}/{len(categories)}] Processing top-level category...")
            self.process_category(category, depth=0)
            time.sleep(0.5)

        elapsed = time.time() - start_time

        for i, product in enumerate(self.all_products, 1):
            product['product_number'] = i

        print(f"\n{'=' * 70}")
        print("SCRAPING COMPLETE!")
        print(f"{'=' * 70}")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print(f"Categories processed: {len(self.processed_categories)}")
        print(f"Total products found: {len(self.all_products)}")

        return {
            'scraped_at': datetime.now().isoformat(),
            'elapsed_seconds': round(elapsed, 2),
            'total_categories': len(self.processed_categories),
            'total_products': len(self.all_products),
            'category_tree': self.category_tree,
            'products': self.all_products
        }

    def save_to_json(self, data: Dict, filename: str = None) -> str:
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"veli_complete_{timestamp}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return filename

    def save_to_csv(self, data: Dict, filename: str = None) -> str:
        import csv

        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"veli_complete_{timestamp}.csv"

        products = data['products']

        if not products:
            return filename

        with open(filename, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['product_number', 'category_name', 'category_id', 'name', 'sku',
                         'current_price', 'original_price', 'discount_percent', 'savings',
                         'currency', 'url']

            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

            for product in products:
                writer.writerow(product)

        return filename

    def print_summary(self, data: Dict):
        print(f"\n{'=' * 70}")
        print("SUMMARY")
        print(f"{'=' * 70}")

        products = data['products']
        categories = data['category_tree']

        print(f"\nTotal Categories: {data['total_categories']}")
        print(f"Total Products: {data['total_products']}")
        print(f"Scraping Time: {data['elapsed_seconds']/60:.1f} minutes")

        if products:
            prices = [p['current_price'] for p in products]
            print(f"\nPrice Range: {min(prices):.2f} ₾ - {max(prices):.2f} ₾")
            print(f"Average Price: {sum(prices)/len(prices):.2f} ₾")

            discounted = [p for p in products if 'discount_percent' in p]
            if discounted:
                print(f"\nProducts on Sale: {len(discounted)} ({len(discounted)/len(products)*100:.1f}%)")

        print(f"\nTop 20 Categories by Product Count:")
        leaf_categories = [c for c in categories if not c.get('has_subcategories', False)]
        leaf_categories.sort(key=lambda x: x.get('product_count', 0), reverse=True)

        for i, cat in enumerate(leaf_categories[:20], 1):
            count = cat.get('product_count', 0)
            name = cat['name']
            print(f"  {i:2d}. {name[:50]:50s} - {count:4d} products")

        if categories:
            max_depth = max(c['depth'] for c in categories)
            print(f"\nMaximum category depth: {max_depth + 1} levels")


def main():
    print("start scraping")

    scraper = ComprehensiveVeliScraper()

    data = scraper.scrape_everything()

    scraper.print_summary(data)

    json_file = scraper.save_to_json(data)
    csv_file = scraper.save_to_csv(data)

    print(f"\n{'=' * 70}")
    print("FILES SAVED")
    print(f"{'=' * 70}")
    print(f"JSON: {json_file}")
    print(f"CSV:  {csv_file}")
    print()
    print("✓ COMPLETE!")


if __name__ == "__main__":
    main()
