import csv
import sys
from collections import defaultdict
from datetime import datetime
import json


class PriceAnalyzer:

    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.data_by_category = defaultdict(list)

    def read_csv(self):
        print(f"Reading data from: {self.csv_file}")

        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                category_name = row.get('category_name', 'Unknown')
                try:
                    current_price = float(row.get('current_price', 0))
                    if current_price > 0:
                        self.data_by_category[category_name].append({
                            'name': row.get('name', ''),
                            'sku': row.get('sku', ''),
                            'current_price': current_price,
                            'original_price': float(row.get('original_price', 0)) if row.get(
                                'original_price') else None,
                            'discount_percent': float(row.get('discount_percent', 0)) if row.get(
                                'discount_percent') else None,
                        })
                except (ValueError, TypeError):
                    continue

        print(f"✓ Loaded data from {len(self.data_by_category)} categories")
        total_products = sum(len(products) for products in self.data_by_category.values())
        print(f"✓ Total products with valid prices: {total_products}")

    def calculate_statistics(self):
        results = []

        for category_name, products in self.data_by_category.items():
            if not products:
                continue

            prices = [p['current_price'] for p in products]

            stats = {
                'category_name': category_name,
                'product_count': len(products),
                'average_price': round(sum(prices) / len(prices), 2),
                'minimum_price': min(prices),
                'maximum_price': max(prices),
                'price_range': round(max(prices) - min(prices), 2),
            }

            on_sale = [p for p in products if p['discount_percent']]
            if on_sale:
                stats['products_on_sale'] = len(on_sale)
                stats['sale_percentage'] = round(len(on_sale) / len(products) * 100, 1)
                stats['average_discount'] = round(
                    sum(p['discount_percent'] for p in on_sale) / len(on_sale), 1
                )

            cheapest = min(products, key=lambda x: x['current_price'])
            most_expensive = max(products, key=lambda x: x['current_price'])

            stats['cheapest_product'] = cheapest['name'][:50]
            stats['cheapest_sku'] = cheapest['sku']
            stats['most_expensive_product'] = most_expensive['name'][:50]
            stats['most_expensive_sku'] = most_expensive['sku']

            results.append(stats)

        results.sort(key=lambda x: x['average_price'], reverse=True)

        return results

    def save_to_csv(self, results, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"price_analysis_{timestamp}.csv"

        fieldnames = [
            'category_name',
            'product_count',
            'average_price',
            'minimum_price',
            'maximum_price',
            'price_range',
            'products_on_sale',
            'sale_percentage',
            'average_discount',
            'cheapest_product',
            'cheapest_sku',
            'most_expensive_product',
            'most_expensive_sku'
        ]

        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)

        print(f"\n✓ Analysis saved to: {output_file}")
        return output_file

    def save_to_json(self, results, output_file=None):
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"price_analysis_{timestamp}.json"

        data = {
            'analyzed_at': datetime.now().isoformat(),
            'total_categories': len(results),
            'total_products': sum(r['product_count'] for r in results),
            'categories': results
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"✓ Analysis saved to: {output_file}")
        return output_file


def main():
    csv_file = sys.argv[1]

    analyzer = PriceAnalyzer(csv_file)

    analyzer.read_csv()

    print("\nCalculating statistics...")
    results = analyzer.calculate_statistics()

    csv_output = analyzer.save_to_csv(results)
    json_output = analyzer.save_to_json(results)

    print("COMPLETE!")
    print(f"  CSV:  {csv_output}")
    print(f"  JSON: {json_output}")


if __name__ == "__main__":
    main()
