#!/usr/bin/env python3
"""
BTC Settlement Markets Fetcher
Fetches BTC-related markets from Kalshi and Polymarket every 15 minutes
"""

import requests
import json
import time
from datetime import datetime
import csv
import re

class MarketFetcher:
    def __init__(self):
        self.kalshi_base = "https://api.elections.kalshi.com/trade-api/v2"
        self.polymarket_base = "https://gamma-api.polymarket.com"
        
    def fetch_kalshi_btc_markets(self):
        """Fetch BTC-related markets from Kalshi by scraping their BTC category page"""
        try:
            # Scrape the Kalshi BTC category page to find all series
            print("Fetching Kalshi BTC category page...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            page_response = requests.get('https://kalshi.com/category/crypto/btc', headers=headers, timeout=10)
            page_response.raise_for_status()
            
            # Extract series tickers from the page content
            content = page_response.text
            
            # Find all series tickers (pattern: KX followed by uppercase letters/numbers)
            series_pattern = r'/(?:markets|events)/(KX[A-Z0-9]+)'
            matches = re.findall(series_pattern, content)
            btc_series_tickers = list(set(matches))  # Remove duplicates
            
            print(f"Found {len(btc_series_tickers)} BTC series: {btc_series_tickers}")
            
            # Now get all open markets for these series
            btc_markets = []
            for series_ticker in btc_series_tickers:
                try:
                    params = {
                        'series_ticker': series_ticker,
                        'status': 'open',
                        'limit': 200
                    }
                    response = requests.get(f"{self.kalshi_base}/markets", params=params, timeout=10)
                    
                    if response.status_code == 404:
                        continue
                        
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'markets' in data:
                        market_count = len(data['markets'])
                        if market_count > 0:
                            print(f"  {series_ticker}: {market_count} markets")
                        
                        for market in data['markets']:
                            btc_markets.append({
                                'exchange': 'Kalshi',
                                'title': market.get('title', ''),
                                'subtitle': market.get('subtitle', ''),
                                'ticker': market.get('ticker'),
                                'event_ticker': market.get('event_ticker'),
                                'series_ticker': series_ticker,
                                'yes_bid': market.get('yes_bid'),
                                'yes_ask': market.get('yes_ask'),
                                'no_bid': market.get('no_bid'),
                                'no_ask': market.get('no_ask'),
                                'last_price': market.get('last_price'),
                                'volume': market.get('volume'),
                                'volume_24h': market.get('volume_24h'),
                                'open_interest': market.get('open_interest'),
                                'close_time': market.get('close_time'),
                                'expiration_time': market.get('expiration_time'),
                                'status': market.get('status'),
                                'timestamp': datetime.now().isoformat()
                            })
                except Exception as e:
                    print(f"  Error fetching series {series_ticker}: {e}")
                    continue
            
            print(f"Total Kalshi markets found: {len(btc_markets)}")
            return btc_markets
        except Exception as e:
            print(f"Error fetching Kalshi markets: {e}")
            print(f"Full error details: {repr(e)}")
            return []
    
    def fetch_polymarket_btc_markets(self):
        """Fetch BTC-related markets from Polymarket using tag filtering"""
        try:
            # Use crypto tag_id=21 with related_tags to get all crypto including Bitcoin
            params = {
                'tag_id': '21',  # Crypto tag
                'related_tags': 'true',
                'active': 'true',
                'closed': 'false',
                'limit': 100,
                'order': 'volume24hr',
                'ascending': 'false'
            }
            
            response = requests.get(
                f"{self.polymarket_base}/events",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            btc_markets = []
            for event in data:
                title = event.get('title', '')
                description = event.get('description', '')
                
                # Filter specifically for Bitcoin/BTC in title or description
                if ('BTC' in title.upper() or 'BITCOIN' in title.upper() or
                    'BTC' in description.upper() or 'BITCOIN' in description.upper()):
                    
                    # Each event can have multiple markets
                    markets = event.get('markets', [])
                    for market in markets:
                        # Parse outcome prices (they're stored as JSON strings)
                        outcome_prices_str = market.get('outcomePrices', '[]')
                        try:
                            if isinstance(outcome_prices_str, str):
                                outcome_prices = json.loads(outcome_prices_str)
                            else:
                                outcome_prices = outcome_prices_str
                        except:
                            outcome_prices = []
                        
                        yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else None
                        no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else None
                        
                        btc_markets.append({
                            'exchange': 'Polymarket',
                            'event_title': title,
                            'market_question': market.get('question'),
                            'market_id': market.get('id'),
                            'condition_id': market.get('conditionId'),
                            'yes_price': yes_price,
                            'no_price': no_price,
                            'volume': event.get('volume'),
                            'volume_24h': event.get('volume24hr'),
                            'liquidity': event.get('liquidity'),
                            'start_date': event.get('startDate'),
                            'end_date': event.get('endDate'),
                            'event_slug': event.get('slug'),
                            'timestamp': datetime.now().isoformat()
                        })
            
            return btc_markets
        except Exception as e:
            print(f"Error fetching Polymarket markets: {e}")
            return []
    
    def fetch_all_markets(self):
        """Fetch markets from both exchanges"""
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching BTC markets...")
        
        kalshi_markets = self.fetch_kalshi_btc_markets()
        polymarket_markets = self.fetch_polymarket_btc_markets()
        
        all_markets = kalshi_markets + polymarket_markets
        
        print(f"Found {len(kalshi_markets)} Kalshi markets")
        print(f"Found {len(polymarket_markets)} Polymarket markets")
        print(f"Total: {len(all_markets)} markets")
        
        return all_markets
    
    def calculate_kalshi_fee(self, price_cents):
        """Calculate Kalshi taker fee: 7% × p × (1-p)"""
        p = price_cents / 100  # Convert cents to decimal probability
        fee_rate = 0.07 * p * (1 - p)
        return fee_rate * 100  # Return as cents
    
    def calculate_polymarket_fee(self, price, is_us=False):
        """Calculate Polymarket fee (simplified to taker fee for comparison)"""
        if is_us:
            return price * 0.001  # 0.1% taker fee
        else:
            # For global, assume 2% on net profits (conservative estimate)
            return price * 0.02
    
    def find_arbitrage_opportunities(self, min_profit_percent=1.0, use_us_poly_fee=False):
        """
        Find arbitrage opportunities between Kalshi and Polymarket
        
        Args:
            min_profit_percent: Minimum profit percentage to report (default 1%)
            use_us_poly_fee: Use US Polymarket fees (0.1%) instead of global (2%)
        """
        print("\n" + "="*80)
        print("SEARCHING FOR ARBITRAGE OPPORTUNITIES")
        print("="*80)
        
        markets = self.fetch_all_markets()
        
        # Organize markets by title/question for matching
        kalshi_dict = {}
        polymarket_dict = {}
        
        for market in markets:
            if market['exchange'] == 'Kalshi':
                # Use title as key (normalized)
                key = market['title'].lower().strip()
                kalshi_dict[key] = market
            else:  # Polymarket
                key = market['event_title'].lower().strip()
                if key not in polymarket_dict:
                    polymarket_dict[key] = []
                polymarket_dict[key].append(market)
        
        opportunities = []
        
        # Try to find matching markets
        print("\nAnalyzing price discrepancies...")
        
        for kalshi_key, kalshi_market in kalshi_dict.items():
            for poly_key, poly_markets in polymarket_dict.items():
                # Simple matching - check if keys are similar (contains similar words)
                kalshi_words = set(kalshi_key.split())
                poly_words = set(poly_key.split())
                
                # If there's significant word overlap, consider it a match
                if len(kalshi_words & poly_words) >= 2 or kalshi_key in poly_key or poly_key in kalshi_key:
                    for poly_market in poly_markets:
                        # Get prices
                        kalshi_yes_ask = kalshi_market.get('yes_ask')
                        kalshi_no_ask = kalshi_market.get('no_ask')
                        poly_yes = poly_market.get('yes_price')
                        poly_no = poly_market.get('no_price')
                        
                        if not all([kalshi_yes_ask, kalshi_no_ask, poly_yes, poly_no]):
                            continue
                        
                        # Convert Polymarket prices (0-1) to cents (0-100) for comparison
                        poly_yes_cents = poly_yes * 100
                        poly_no_cents = poly_no * 100
                        
                        # Strategy 1: Buy YES on Kalshi, Buy NO on Polymarket
                        kalshi_yes_cost = kalshi_yes_ask
                        kalshi_yes_fee = self.calculate_kalshi_fee(kalshi_yes_ask)
                        poly_no_cost = poly_no_cents
                        poly_no_fee = self.calculate_polymarket_fee(poly_no_cents, use_us_poly_fee)
                        
                        total_cost_1 = kalshi_yes_cost + kalshi_yes_fee + poly_no_cost + poly_no_fee
                        profit_1 = 100 - total_cost_1  # Guaranteed payout is 100 cents
                        profit_pct_1 = (profit_1 / total_cost_1) * 100 if total_cost_1 > 0 else 0
                        
                        # Strategy 2: Buy NO on Kalshi, Buy YES on Polymarket
                        kalshi_no_cost = kalshi_no_ask
                        kalshi_no_fee = self.calculate_kalshi_fee(kalshi_no_ask)
                        poly_yes_cost = poly_yes_cents
                        poly_yes_fee = self.calculate_polymarket_fee(poly_yes_cents, use_us_poly_fee)
                        
                        total_cost_2 = kalshi_no_cost + kalshi_no_fee + poly_yes_cost + poly_yes_fee
                        profit_2 = 100 - total_cost_2
                        profit_pct_2 = (profit_2 / total_cost_2) * 100 if total_cost_2 > 0 else 0
                        
                        # Check if either strategy is profitable
                        if profit_pct_1 >= min_profit_percent:
                            opportunities.append({
                                'strategy': 'Buy YES on Kalshi, Buy NO on Polymarket',
                                'kalshi_market': kalshi_market['title'],
                                'kalshi_ticker': kalshi_market['ticker'],
                                'poly_market': poly_market['event_title'],
                                'kalshi_price': kalshi_yes_ask,
                                'kalshi_fee': kalshi_yes_fee,
                                'poly_price': poly_no_cents,
                                'poly_fee': poly_no_fee,
                                'total_cost': total_cost_1,
                                'profit': profit_1,
                                'profit_pct': profit_pct_1
                            })
                        
                        if profit_pct_2 >= min_profit_percent:
                            opportunities.append({
                                'strategy': 'Buy NO on Kalshi, Buy YES on Polymarket',
                                'kalshi_market': kalshi_market['title'],
                                'kalshi_ticker': kalshi_market['ticker'],
                                'poly_market': poly_market['event_title'],
                                'kalshi_price': kalshi_no_ask,
                                'kalshi_fee': kalshi_no_fee,
                                'poly_price': poly_yes_cents,
                                'poly_fee': poly_yes_fee,
                                'total_cost': total_cost_2,
                                'profit': profit_2,
                                'profit_pct': profit_pct_2
                            })
        
        # Sort by profit percentage
        opportunities.sort(key=lambda x: x['profit_pct'], reverse=True)
        
        # Display results
        print("\n" + "="*80)
        if opportunities:
            print(f"FOUND {len(opportunities)} ARBITRAGE OPPORTUNITIES")
            print("="*80)
            
            for i, opp in enumerate(opportunities, 1):
                print(f"\n#{i} - {opp['profit_pct']:.2f}% profit")
                print(f"Strategy: {opp['strategy']}")
                print(f"Kalshi:     {opp['kalshi_market'][:60]}")
                print(f"            Ticker: {opp['kalshi_ticker']}")
                print(f"            Price: {opp['kalshi_price']:.1f}¢ + Fee: {opp['kalshi_fee']:.2f}¢")
                print(f"Polymarket: {opp['poly_market'][:60]}")
                print(f"            Price: {opp['poly_price']:.1f}¢ + Fee: {opp['poly_fee']:.2f}¢")
                print(f"Total Cost: {opp['total_cost']:.2f}¢")
                print(f"Profit:     {opp['profit']:.2f}¢ ({opp['profit_pct']:.2f}%)")
                print("-" * 80)
        else:
            print("NO PROFITABLE ARBITRAGE OPPORTUNITIES FOUND")
            print(f"(Minimum profit threshold: {min_profit_percent}%)")
            print("="*80)
        
        return opportunities
    
    def save_to_json(self, markets, filename="btc_markets.json"):
        """Save markets to JSON file"""
        try:
            # Load existing data if file exists
            try:
                with open(filename, 'r') as f:
                    existing_data = json.load(f)
            except FileNotFoundError:
                existing_data = []
            
            # Append new markets
            existing_data.extend(markets)
            
            # Save back to file
            with open(filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            print(f"Saved to {filename}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def save_to_csv(self, markets, filename="btc_markets.csv"):
        """Save markets to CSV file"""
        try:
            if not markets:
                return
            
            # Check if file exists to determine if we need headers
            try:
                with open(filename, 'r'):
                    write_header = False
            except FileNotFoundError:
                write_header = True
            
            # Append to CSV
            with open(filename, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=markets[0].keys())
                if write_header:
                    writer.writeheader()
                writer.writerows(markets)
            
            print(f"Saved to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def run_continuous(self, interval_minutes=15):
        """Run continuously, fetching every interval_minutes"""
        print(f"Starting continuous monitoring (every {interval_minutes} minutes)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                markets = self.fetch_all_markets()
                
                if markets:
                    self.save_to_json(markets)
                    self.save_to_csv(markets)
                    
                    # Display summary
                    print("\nCurrent markets:")
                    for i, market in enumerate(markets[:10], 1):  # Show first 10
                        if market['exchange'] == 'Kalshi':
                            print(f"  {i}. [{market['exchange']}] {market['title']}")
                            print(f"     Yes: {market.get('yes_bid', 'N/A')}¢ | No: {market.get('no_bid', 'N/A')}¢ | Vol: {market.get('volume', 0)}")
                        else:  # Polymarket
                            print(f"  {i}. [{market['exchange']}] {market['event_title']}")
                            print(f"     Yes: {market.get('yes_price', 'N/A')} | No: {market.get('no_price', 'N/A')} | Vol: {market.get('volume', 0)}")
                    if len(markets) > 10:
                        print(f"  ... and {len(markets) - 10} more")
                
                print(f"\nNext fetch in {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n\nStopped by user")
    
    def run_once(self):
        """Run a single fetch"""
        markets = self.fetch_all_markets()
        
        if markets:
            self.save_to_json(markets)
            self.save_to_csv(markets)
            
            # Display all markets
            print("\n" + "="*80)
            for i, market in enumerate(markets, 1):
                print(f"\n{i}. [{market['exchange']}]")
                if market['exchange'] == 'Kalshi':
                    print(f"Title: {market['title']}")
                    if market.get('subtitle'):
                        print(f"Subtitle: {market['subtitle']}")
                    print(f"Ticker: {market.get('ticker')}")
                    print(f"Yes Bid: {market.get('yes_bid')}¢ | Yes Ask: {market.get('yes_ask')}¢")
                    print(f"No Bid: {market.get('no_bid')}¢ | No Ask: {market.get('no_ask')}¢")
                    print(f"Last Price: {market.get('last_price')}¢")
                    print(f"Volume: {market.get('volume')} | 24h Volume: {market.get('volume_24h')}")
                    print(f"Open Interest: {market.get('open_interest')}")
                    print(f"Close Time: {market.get('close_time')}")
                else:  # Polymarket
                    print(f"Event: {market['event_title']}")
                    print(f"Question: {market.get('market_question')}")
                    print(f"Market ID: {market.get('market_id')}")
                    print(f"Yes Price: {market.get('yes_price')} | No Price: {market.get('no_price')}")
                    print(f"Volume: {market.get('volume')}")
                    print(f"Liquidity: {market.get('liquidity')}")
                    print(f"End Date: {market.get('end_date')}")
                    print(f"URL: https://polymarket.com/event/{market.get('event_slug')}")
        else:
            print("No BTC markets found")

def main():
    fetcher = MarketFetcher()
    
    print("BTC Settlement Markets Fetcher")
    print("="*80)
    print("\nOptions:")
    print("1. Run once (fetch now and exit)")
    print("2. Run continuously (fetch every 15 minutes)")
    print("3. Find arbitrage opportunities")
    
    choice = input("\nEnter choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        fetcher.run_once()
    elif choice == "2":
        fetcher.run_continuous(interval_minutes=15)
    elif choice == "3":
        min_profit = input("Minimum profit % (default 1.0): ").strip()
        min_profit = float(min_profit) if min_profit else 1.0
        
        use_us = input("Use US Polymarket fees? (y/n, default n): ").strip().lower()
        use_us_fees = use_us == 'y'
        
        fetcher.find_arbitrage_opportunities(min_profit_percent=min_profit, use_us_poly_fee=use_us_fees)
    else:
        print("Invalid choice. Running once by default...")
        fetcher.run_once()

if __name__ == "__main__":
    main()
