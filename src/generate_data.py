"""
Synthetic E-Commerce Dataset Generator
======================================
Generates realistic raw e-commerce data for an end-to-end data engineering pipeline:
- Customers (data/raw/customers.csv)
- Products (data/raw/products.csv)
- Orders (data/raw/orders.csv)
- Order Items (data/raw/order_items.csv)

Intentionally introduced data-quality issues (for downstream pipeline validation):
1. Duplicate customer rows: ~15 duplicate records in customers.csv (~0.75%)
2. Missing customer city values: ~15 null values in customers.csv (~0.75%)
3. Missing product prices: 2 null prices in products.csv (2.0%)
4. Invalid order item quantities: ~150 rows with 0 or negative quantities in order_items.csv (~0.6%)
5. Missing payment methods: ~60 null payment methods in orders.csv (~0.6%)
6. Orphan customer IDs: ~50 orders referencing non-existent customer_id (99999) (~0.5%)
7. Orphan product IDs: ~120 order items referencing non-existent product_id (9999) (~0.5%)
"""

from datetime import datetime, timedelta
from pathlib import Path
import random
import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# CONSTANTS & CONFIGURATION
# -----------------------------------------------------------------------------
RANDOM_SEED = 42

NUM_CUSTOMERS = 2000
NUM_PRODUCTS = 100
NUM_ORDERS = 10000

OUTPUT_DIR = Path("data/raw")

# Seed both random generators for complete reproducibility
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Realistic Indian demographic pools
FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Aayush",
    "Krishna", "Ishan", "Shaurya", "Atharv", "Advik", "Pranav", "Aryan", "Dhruv",
    "Kabir", "Ritvik", "Rohan", "Rahul", "Amit", "Vikram", "Suresh", "Rajesh",
    "Alok", "Nikhil", "Kunal", "Manish", "Varun", "Gaurav", "Abhishek", "Sanjay",
    "Deepak", "Saanvi", "Aanya", "Aadhya", "Aaradhya", "Ananya", "Pari", "Diya",
    "Pihu", "Prisha", "Riya", "Anvi", "Sneha", "Tanvi", "Kavya", "Meera", "Pooja",
    "Neha", "Priya", "Ritu", "Simran", "Divya", "Shreya", "Swati", "Jyoti", "Sunita",
    "Preeti", "Shruti", "Deepa", "Rekha", "Anita", "Kiran", "Meenakshi", "Pooja"
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Patel", "Mehta", "Shah", "Singh", "Kumar", "Rao",
    "Reddy", "Nair", "Iyer", "Joshi", "Kulkarni", "Deshmukh", "Patil", "Banerjee",
    "Chatterjee", "Mukherjee", "Das", "Ghosh", "Roy", "Agarwal", "Jain", "Mishra",
    "Pandey", "Tiwari", "Yadav", "Chauhan", "Rathore", "Bhat", "Nambiar", "Pillai",
    "Choudhury", "Goswami", "Menon", "Saxena", "Kapoor", "Bose", "Dutta"
]

INDIAN_LOCATIONS = [
    {"city": "Mumbai", "state": "Maharashtra"},
    {"city": "Pune", "state": "Maharashtra"},
    {"city": "Nagpur", "state": "Maharashtra"},
    {"city": "Delhi", "state": "Delhi"},
    {"city": "Bengaluru", "state": "Karnataka"},
    {"city": "Mysuru", "state": "Karnataka"},
    {"city": "Hyderabad", "state": "Telangana"},
    {"city": "Warangal", "state": "Telangana"},
    {"city": "Chennai", "state": "Tamil Nadu"},
    {"city": "Coimbatore", "state": "Tamil Nadu"},
    {"city": "Kolkata", "state": "West Bengal"},
    {"city": "Howrah", "state": "West Bengal"},
    {"city": "Ahmedabad", "state": "Gujarat"},
    {"city": "Surat", "state": "Gujarat"},
    {"city": "Vadodara", "state": "Gujarat"},
    {"city": "Jaipur", "state": "Rajasthan"},
    {"city": "Jodhpur", "state": "Rajasthan"},
    {"city": "Lucknow", "state": "Uttar Pradesh"},
    {"city": "Kanpur", "state": "Uttar Pradesh"},
    {"city": "Noida", "state": "Uttar Pradesh"},
    {"city": "Chandigarh", "state": "Punjab"},
    {"city": "Ludhiana", "state": "Punjab"},
    {"city": "Indore", "state": "Madhya Pradesh"},
    {"city": "Bhopal", "state": "Madhya Pradesh"},
    {"city": "Kochi", "state": "Kerala"},
    {"city": "Thiruvananthapuram", "state": "Kerala"},
    {"city": "Patna", "state": "Bihar"},
    {"city": "Bhubaneswar", "state": "Odisha"},
    {"city": "Guwahati", "state": "Assam"},
    {"city": "Ranchi", "state": "Jharkhand"}
]

PRODUCT_CATALOG = [
    # Electronics (20 products)
    {"name": "Smartphone Pro Max", "category": "Electronics", "min_price": 24999.0, "max_price": 79999.0},
    {"name": "Wireless Noise Cancelling Earbuds", "category": "Electronics", "min_price": 1999.0, "max_price": 8999.0},
    {"name": "Ultra-Slim 14-inch Laptop", "category": "Electronics", "min_price": 38999.0, "max_price": 74999.0},
    {"name": "Fitness Smartwatch with SpO2", "category": "Electronics", "min_price": 1799.0, "max_price": 5499.0},
    {"name": "Portable Bluetooth Speaker 20W", "category": "Electronics", "min_price": 1299.0, "max_price": 3999.0},
    {"name": "43-inch 4K Ultra HD Smart TV", "category": "Electronics", "min_price": 21999.0, "max_price": 34999.0},
    {"name": "Ergonomic Optical Gaming Mouse", "category": "Electronics", "min_price": 599.0, "max_price": 1999.0},
    {"name": "Mechanical RGB Gaming Keyboard", "category": "Electronics", "min_price": 1899.0, "max_price": 4999.0},
    {"name": "10-inch Android Tablet", "category": "Electronics", "min_price": 9999.0, "max_price": 22999.0},
    {"name": "20000mAh Fast Charging Power Bank", "category": "Electronics", "min_price": 1199.0, "max_price": 2499.0},
    {"name": "Over-Ear Studio Headphones", "category": "Electronics", "min_price": 2499.0, "max_price": 9999.0},
    {"name": "1TB External Solid State Drive", "category": "Electronics", "min_price": 5499.0, "max_price": 8999.0},
    {"name": "7-in-1 USB-C Multiport Adapter", "category": "Electronics", "min_price": 1499.0, "max_price": 2999.0},
    {"name": "1080p Full HD Streaming Webcam", "category": "Electronics", "min_price": 1299.0, "max_price": 3299.0},
    {"name": "Dual Band Gigabit Wi-Fi 6 Router", "category": "Electronics", "min_price": 2199.0, "max_price": 4999.0},
    {"name": "Mini Portable LED Projector", "category": "Electronics", "min_price": 6999.0, "max_price": 14999.0},
    {"name": "Smart Wi-Fi Home Security Camera", "category": "Electronics", "min_price": 1899.0, "max_price": 3999.0},
    {"name": "Wireless Charging Pad 15W", "category": "Electronics", "min_price": 799.0, "max_price": 1899.0},
    {"name": "Smart Wi-Fi Plug 16A with Energy Monitoring", "category": "Electronics", "min_price": 699.0, "max_price": 1499.0},
    {"name": "Action Camera 4K Waterproof", "category": "Electronics", "min_price": 4999.0, "max_price": 12999.0},

    # Clothing (18 products)
    {"name": "Men's Regular Fit Cotton T-Shirt", "category": "Clothing", "min_price": 399.0, "max_price": 899.0},
    {"name": "Women's Floral Anarkali Kurti", "category": "Clothing", "min_price": 799.0, "max_price": 1999.0},
    {"name": "Men's Slim Fit Stretch Denim Jeans", "category": "Clothing", "min_price": 1199.0, "max_price": 2799.0},
    {"name": "Women's High-Waist Skinny Jeans", "category": "Clothing", "min_price": 1099.0, "max_price": 2499.0},
    {"name": "Men's Formal Pure Cotton Shirt", "category": "Clothing", "min_price": 899.0, "max_price": 2199.0},
    {"name": "Women's Casual Cotton Maxi Dress", "category": "Clothing", "min_price": 999.0, "max_price": 2299.0},
    {"name": "Fleece Pullover Winter Hoodie", "category": "Clothing", "min_price": 999.0, "max_price": 2499.0},
    {"name": "Men's Quick-Dry Sports Shorts", "category": "Clothing", "min_price": 499.0, "max_price": 1099.0},
    {"name": "Pure Banarasi Art Silk Saree", "category": "Clothing", "min_price": 1499.0, "max_price": 4999.0},
    {"name": "Unisex Classic Denim Jacket", "category": "Clothing", "min_price": 1699.0, "max_price": 3499.0},
    {"name": "Men's Casual Chino Trousers", "category": "Clothing", "min_price": 999.0, "max_price": 2199.0},
    {"name": "Breathable Activewear Sports Bra", "category": "Clothing", "min_price": 599.0, "max_price": 1299.0},
    {"name": "Cotton Ankle Length Socks (Pack of 3)", "category": "Clothing", "min_price": 299.0, "max_price": 599.0},
    {"name": "Athletic Training Track Pants", "category": "Clothing", "min_price": 699.0, "max_price": 1499.0},
    {"name": "Pure Linen Casual Button-Down Shirt", "category": "Clothing", "min_price": 1299.0, "max_price": 2999.0},
    {"name": "Soft Merino Woolen Muffler Scarf", "category": "Clothing", "min_price": 499.0, "max_price": 1299.0},
    {"name": "Lightweight Waterproof Rain Jacket", "category": "Clothing", "min_price": 899.0, "max_price": 1999.0},
    {"name": "Women's Embroidered Palazzo Pants", "category": "Clothing", "min_price": 649.0, "max_price": 1499.0},

    # Home & Kitchen (16 products)
    {"name": "Stainless Steel Insulated Water Bottle 1L", "category": "Home & Kitchen", "min_price": 499.0, "max_price": 1199.0},
    {"name": "Hard Anodized Non-Stick Frying Pan", "category": "Home & Kitchen", "min_price": 899.0, "max_price": 1899.0},
    {"name": "Ceramic Tea & Coffee Mug Set (Pack of 6)", "category": "Home & Kitchen", "min_price": 599.0, "max_price": 1299.0},
    {"name": "Stainless Steel Kitchen Chef Knife 8-inch", "category": "Home & Kitchen", "min_price": 399.0, "max_price": 999.0},
    {"name": "Cordless Electric Kettle 1.8L", "category": "Home & Kitchen", "min_price": 799.0, "max_price": 1799.0},
    {"name": "Digital Air Fryer 4.5L", "category": "Home & Kitchen", "min_price": 4299.0, "max_price": 7999.0},
    {"name": "100% Pure Cotton King Size Bedsheet", "category": "Home & Kitchen", "min_price": 899.0, "max_price": 2199.0},
    {"name": "Microfiber Dust Cleaning Towels (Pack of 4)", "category": "Home & Kitchen", "min_price": 249.0, "max_price": 549.0},
    {"name": "Soy Wax Scented Aromatherapy Candles", "category": "Home & Kitchen", "min_price": 449.0, "max_price": 999.0},
    {"name": "Airtight Food Storage Containers (Set of 6)", "category": "Home & Kitchen", "min_price": 699.0, "max_price": 1599.0},
    {"name": "Revolving Spice Rack Organizer (16 Jars)", "category": "Home & Kitchen", "min_price": 599.0, "max_price": 1299.0},
    {"name": "Silent Sweep Quartz Wall Clock 12-inch", "category": "Home & Kitchen", "min_price": 499.0, "max_price": 1199.0},
    {"name": "Vacuum Insulated Flask 750ml", "category": "Home & Kitchen", "min_price": 649.0, "max_price": 1399.0},
    {"name": "750W Mixer Grinder with 3 Jars", "category": "Home & Kitchen", "min_price": 2499.0, "max_price": 4999.0},
    {"name": "Silicone Non-Stick Baking Mat", "category": "Home & Kitchen", "min_price": 329.0, "max_price": 699.0},
    {"name": "Stainless Steel Cutlery Set (24 Pieces)", "category": "Home & Kitchen", "min_price": 799.0, "max_price": 1799.0},

    # Books (14 products)
    {"name": "Designing Data-Intensive Applications", "category": "Books", "min_price": 1199.0, "max_price": 1899.0},
    {"name": "Clean Code: A Handbook of Agile Craftsmanship", "category": "Books", "min_price": 699.0, "max_price": 1199.0},
    {"name": "Python for Data Analysis (3rd Edition)", "category": "Books", "min_price": 899.0, "max_price": 1499.0},
    {"name": "The Psychology of Money", "category": "Books", "min_price": 299.0, "max_price": 499.0},
    {"name": "Atomic Habits: An Easy & Proven Way", "category": "Books", "min_price": 399.0, "max_price": 699.0},
    {"name": "Sapiens: A Brief History of Humankind", "category": "Books", "min_price": 349.0, "max_price": 599.0},
    {"name": "Deep Work: Rules for Focused Success", "category": "Books", "min_price": 299.0, "max_price": 549.0},
    {"name": "Rich Dad Poor Dad", "category": "Books", "min_price": 249.0, "max_price": 449.0},
    {"name": "Think and Grow Rich", "category": "Books", "min_price": 199.0, "max_price": 349.0},
    {"name": "The Alchemist", "category": "Books", "min_price": 229.0, "max_price": 399.0},
    {"name": "Ikigai: The Japanese Secret to a Long Life", "category": "Books", "min_price": 299.0, "max_price": 499.0},
    {"name": "Good to Great: Why Some Companies Make the Leap", "category": "Books", "min_price": 449.0, "max_price": 749.0},
    {"name": "Zero to One: Notes on Startups", "category": "Books", "min_price": 319.0, "max_price": 549.0},
    {"name": "To Kill a Mockingbird", "category": "Books", "min_price": 249.0, "max_price": 449.0},

    # Sports (12 products)
    {"name": "Anti-Slip High Density Yoga Mat 6mm", "category": "Sports", "min_price": 649.0, "max_price": 1499.0},
    {"name": "Adjustable Dumbbells Set 20kg", "category": "Sports", "min_price": 2499.0, "max_price": 5499.0},
    {"name": "Resistance Workout Bands (Set of 5)", "category": "Sports", "min_price": 499.0, "max_price": 1199.0},
    {"name": "Kashmir Willow Full Size Cricket Bat", "category": "Sports", "min_price": 1199.0, "max_price": 2999.0},
    {"name": "All-Weather Football (Size 5)", "category": "Sports", "min_price": 599.0, "max_price": 1299.0},
    {"name": "Graphite Badminton Racket Twin Pack", "category": "Sports", "min_price": 899.0, "max_price": 2199.0},
    {"name": "Speed Skipping Jump Rope with Ball Bearings", "category": "Sports", "min_price": 249.0, "max_price": 599.0},
    {"name": "Heavy Duty Canvas Gym Duffle Bag", "category": "Sports", "min_price": 799.0, "max_price": 1699.0},
    {"name": "Aerodynamic Bicycle Helmet", "category": "Sports", "min_price": 999.0, "max_price": 2199.0},
    {"name": "Anti-Fog UV Protection Swimming Goggles", "category": "Sports", "min_price": 399.0, "max_price": 899.0},
    {"name": "High Density Muscle Foam Roller 18-inch", "category": "Sports", "min_price": 649.0, "max_price": 1399.0},
    {"name": "Pressurized Championship Tennis Balls (Can of 3)", "category": "Sports", "min_price": 349.0, "max_price": 649.0},

    # Beauty (10 products)
    {"name": "Vitamin C Radiance Face Serum 30ml", "category": "Beauty", "min_price": 499.0, "max_price": 1199.0},
    {"name": "Gentle Hydrating Foaming Face Wash 150ml", "category": "Beauty", "min_price": 249.0, "max_price": 599.0},
    {"name": "Ultra-Matte Sunscreen Gel SPF 50 50g", "category": "Beauty", "min_price": 399.0, "max_price": 799.0},
    {"name": "Long-Stay Velvet Matte Lipstick", "category": "Beauty", "min_price": 299.0, "max_price": 749.0},
    {"name": "Bamboo Charcoal Peel-Off Face Mask", "category": "Beauty", "min_price": 229.0, "max_price": 499.0},
    {"name": "Onion & Bhringraj Anti-Hairfall Shampoo 300ml", "category": "Beauty", "min_price": 349.0, "max_price": 699.0},
    {"name": "Cold-Pressed Moroccan Argan Hair Oil 100ml", "category": "Beauty", "min_price": 499.0, "max_price": 1099.0},
    {"name": "Deep Nourishing Cocoa Butter Body Lotion 400ml", "category": "Beauty", "min_price": 299.0, "max_price": 649.0},
    {"name": "Natural Walnut Gentle Face Scrub 100g", "category": "Beauty", "min_price": 199.0, "max_price": 449.0},
    {"name": "Caffeine Infused Under Eye Cream 20g", "category": "Beauty", "min_price": 349.0, "max_price": 799.0},

    # Grocery (10 products)
    {"name": "Organic Darjeeling Whole Leaf Green Tea 250g", "category": "Grocery", "min_price": 299.0, "max_price": 649.0},
    {"name": "Cold-Pressed Extra Virgin Olive Oil 1L", "category": "Grocery", "min_price": 799.0, "max_price": 1599.0},
    {"name": "100% Durum Wheat Fusilli Pasta 500g", "category": "Grocery", "min_price": 149.0, "max_price": 299.0},
    {"name": "70% Single Origin Dark Chocolate Bar 100g", "category": "Grocery", "min_price": 179.0, "max_price": 349.0},
    {"name": "Premium California Whole Almonds 500g", "category": "Grocery", "min_price": 449.0, "max_price": 799.0},
    {"name": "Roasted Granulated Instant Coffee 100g", "category": "Grocery", "min_price": 249.0, "max_price": 549.0},
    {"name": "100% Pure Raw Forest Honey 500g", "category": "Grocery", "min_price": 299.0, "max_price": 599.0},
    {"name": "Whole Grain Rolled Oats 1kg", "category": "Grocery", "min_price": 199.0, "max_price": 399.0},
    {"name": "Creamy Unsweetened Peanut Butter 1kg", "category": "Grocery", "min_price": 329.0, "max_price": 599.0},
    {"name": "Aged Long Grain Traditional Basmati Rice 5kg", "category": "Grocery", "min_price": 599.0, "max_price": 1099.0}
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Net Banking"]
PAYMENT_WEIGHTS = [0.44, 0.22, 0.14, 0.12, 0.08]

ORDER_STATUSES = ["Completed", "Shipped", "Processing", "Cancelled", "Returned"]
STATUS_WEIGHTS = [0.65, 0.16, 0.07, 0.07, 0.05]


# -----------------------------------------------------------------------------
# DATA GENERATION FUNCTIONS
# -----------------------------------------------------------------------------
def generate_customers(num_customers: int = NUM_CUSTOMERS) -> pd.DataFrame:
    """
    Generate synthetic customers with realistic Indian demographics and valid signup dates.

    Args:
        num_customers: Number of unique customer records to create.

    Returns:
        pd.DataFrame: Customer dataset with columns:
            [customer_id, customer_name, city, state, country, signup_date]
    """
    customer_ids = list(range(1, num_customers + 1))

    # Realistic names by combining first & last name pools
    first_choice = np.random.choice(FIRST_NAMES, size=num_customers)
    last_choice = np.random.choice(LAST_NAMES, size=num_customers)
    customer_names = [f"{f} {l}" for f, l in zip(first_choice, last_choice)]

    # Pick realistic Indian cities and corresponding states
    loc_indices = np.random.choice(len(INDIAN_LOCATIONS), size=num_customers)
    cities = [INDIAN_LOCATIONS[i]["city"] for i in loc_indices]
    states = [INDIAN_LOCATIONS[i]["state"] for i in loc_indices]

    # Country primarily India (99.5% India, 0.5% international)
    countries = np.random.choice(
        ["India", "United States", "United Arab Emirates", "Singapore"],
        size=num_customers,
        p=[0.995, 0.002, 0.002, 0.001]
    )

    # Signup dates spanning 3 years prior up to roughly 6 months ago (e.g. 2022-01-01 to 2024-06-30)
    start_signup = datetime(2022, 1, 1)
    end_signup = datetime(2024, 6, 30)
    total_days = (end_signup - start_signup).days
    random_days = np.random.randint(0, total_days + 1, size=num_customers)
    signup_dates = [
        (start_signup + timedelta(days=int(d))).strftime("%Y-%m-%d")
        for d in random_days
    ]

    return pd.DataFrame({
        "customer_id": customer_ids,
        "customer_name": customer_names,
        "city": cities,
        "state": states,
        "country": countries,
        "signup_date": signup_dates
    })


def generate_products(num_products: int = NUM_PRODUCTS) -> pd.DataFrame:
    """
    Generate synthetic product catalog across 7 realistic e-commerce categories.

    Args:
        num_products: Number of unique products to create (default 100).

    Returns:
        pd.DataFrame: Product dataset with columns:
            [product_id, product_name, category, price]
    """
    # Exactly 100 catalog products predefined
    catalog = PRODUCT_CATALOG[:num_products]

    product_ids = list(range(1, len(catalog) + 1))
    product_names = [p["name"] for p in catalog]
    categories = [p["category"] for p in catalog]

    # Generate realistic prices within defined min/max bounds, rounded to 2 decimal places
    prices = [
        round(float(np.random.uniform(p["min_price"], p["max_price"])), 2)
        for p in catalog
    ]

    return pd.DataFrame({
        "product_id": product_ids,
        "product_name": product_names,
        "category": categories,
        "price": prices
    })


def generate_orders(customers_df: pd.DataFrame, num_orders: int = NUM_ORDERS) -> pd.DataFrame:
    """
    Generate synthetic orders linked to valid customers spanning approximately the last 2 years.

    Args:
        customers_df: Customer dataset used to sample realistic customer IDs and enforce chronology.
        num_orders: Number of order records to create (default 10,000).

    Returns:
        pd.DataFrame: Orders dataset with columns:
            [order_id, customer_id, order_date, payment_method, order_status]
    """
    order_ids = list(range(1, num_orders + 1))

    # Sample customer_ids from existing customers with some repeat shoppers
    customer_ids_pool = customers_df["customer_id"].values
    chosen_customer_ids = np.random.choice(customer_ids_pool, size=num_orders)

    # Pre-map customer signup dates to guarantee order_date >= signup_date
    signup_dict = dict(zip(customers_df["customer_id"], pd.to_datetime(customers_df["signup_date"])))
    pipeline_end_date = datetime(2026, 1, 31)

    order_dates = []
    for cid in chosen_customer_ids:
        c_signup = signup_dict[cid]
        # Order happens between customer signup date and pipeline_end_date
        delta_days = max(1, (pipeline_end_date - c_signup).days)
        offset = int(np.random.randint(0, delta_days))
        o_date = c_signup + timedelta(days=offset)
        order_dates.append(o_date.strftime("%Y-%m-%d"))

    payment_methods = np.random.choice(
        PAYMENT_METHODS,
        size=num_orders,
        p=PAYMENT_WEIGHTS
    )

    order_statuses = np.random.choice(
        ORDER_STATUSES,
        size=num_orders,
        p=STATUS_WEIGHTS
    )

    return pd.DataFrame({
        "order_id": order_ids,
        "customer_id": chosen_customer_ids,
        "order_date": order_dates,
        "payment_method": payment_methods,
        "order_status": order_statuses
    })


def generate_order_items(orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate line items for each order, sampling 1 to 5 distinct products per order.

    Args:
        orders_df: Orders dataset to generate items for.
        products_df: Products dataset providing product_id and unit_price lookup.

    Returns:
        pd.DataFrame: Order items dataset with columns:
            [order_id, product_id, quantity, unit_price]
    """
    price_dict = dict(zip(products_df["product_id"], products_df["price"]))
    all_product_ids = products_df["product_id"].values

    # Distribution of number of distinct items per order (1 to 5)
    # Average ~2.4 items/order -> ~24,000 line items total
    num_items_choices = [1, 2, 3, 4, 5]
    num_items_weights = [0.35, 0.30, 0.20, 0.10, 0.05]

    item_order_ids = []
    item_product_ids = []
    item_quantities = []
    item_unit_prices = []

    # Distribution of quantity per item (1 to 5)
    qty_choices = [1, 2, 3, 4, 5]
    qty_weights = [0.55, 0.25, 0.12, 0.05, 0.03]

    for order_id in orders_df["order_id"]:
        k = int(np.random.choice(num_items_choices, p=num_items_weights))
        chosen_products = np.random.choice(all_product_ids, size=k, replace=False)
        quantities = np.random.choice(qty_choices, size=k, p=qty_weights)

        for pid, qty in zip(chosen_products, quantities):
            item_order_ids.append(order_id)
            item_product_ids.append(pid)
            item_quantities.append(int(qty))
            item_unit_prices.append(price_dict[pid])

    return pd.DataFrame({
        "order_id": item_order_ids,
        "product_id": item_product_ids,
        "quantity": item_quantities,
        "unit_price": item_unit_prices
    })


# -----------------------------------------------------------------------------
# DATA QUALITY INJECTION (FOR DATA VALIDATION STAGE)
# -----------------------------------------------------------------------------
def introduce_data_quality_issues(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Intentionally inject a small number of realistic data quality issues (0.5% - 1.0%)
    into the raw datasets to demonstrate data validation and cleaning downstream.

    Issues introduced:
    1. Duplicate customer rows (~0.75%)
    2. Missing customer city values (~0.75%)
    3. Missing product prices (2%)
    4. Invalid quantities (0 or negative) in order items (~0.6%)
    5. Missing payment methods in orders (~0.6%)
    6. Orphan customer IDs in orders (~0.5%)
    7. Orphan product IDs in order items (~0.5%)

    Returns:
        Tuple of modified (customers_df, products_df, orders_df, order_items_df, stats_dict)
    """
    customers_mod = customers_df.copy()
    products_mod = products_df.copy()
    orders_mod = orders_df.copy()
    order_items_mod = order_items_df.copy()

    stats = {}

    # Issue 1: Duplicate customer rows (15 duplicate rows, ~0.75%)
    num_duplicates = 15
    dup_indices = np.random.choice(customers_mod.index, size=num_duplicates, replace=False)
    duplicates = customers_mod.loc[dup_indices].copy()
    customers_mod = pd.concat([customers_mod, duplicates], ignore_index=True)
    stats["duplicate_customers"] = num_duplicates

    # Issue 2: Missing customer city values (15 records, ~0.75%)
    num_missing_cities = 15
    city_null_indices = np.random.choice(customers_mod.index[:len(customers_df)], size=num_missing_cities, replace=False)
    customers_mod.loc[city_null_indices, "city"] = np.nan
    stats["missing_customer_cities"] = num_missing_cities

    # Issue 3: Missing product prices (2 records, 2.0% of 100 products)
    num_missing_prices = 2
    price_null_indices = np.random.choice(products_mod.index, size=num_missing_prices, replace=False)
    products_mod.loc[price_null_indices, "price"] = np.nan
    stats["missing_product_prices"] = num_missing_prices

    # Issue 4: Invalid quantities in order items (150 rows: 100 zeros, 50 negatives; ~0.6%)
    num_invalid_qty = 150
    invalid_qty_indices = np.random.choice(order_items_mod.index, size=num_invalid_qty, replace=False)
    zero_indices = invalid_qty_indices[:100]
    negative_indices = invalid_qty_indices[100:]
    order_items_mod.loc[zero_indices, "quantity"] = 0
    order_items_mod.loc[negative_indices, "quantity"] = np.random.choice([-1, -2, -3], size=len(negative_indices))
    stats["invalid_order_item_quantities"] = num_invalid_qty

    # Issue 5: Missing payment methods in orders (60 records, ~0.6%)
    num_missing_payments = 60
    missing_payment_indices = np.random.choice(orders_mod.index, size=num_missing_payments, replace=False)
    orders_mod.loc[missing_payment_indices, "payment_method"] = np.nan
    stats["missing_payment_methods"] = num_missing_payments

    # Issue 6: Orphan customer IDs in orders (50 records, ~0.5% - customer_id=99999)
    num_orphan_customers = 50
    orphan_customer_indices = np.random.choice(orders_mod.index, size=num_orphan_customers, replace=False)
    orders_mod.loc[orphan_customer_indices, "customer_id"] = 99999
    stats["orphan_order_customer_ids"] = num_orphan_customers

    # Issue 7: Orphan product IDs in order items (120 records, ~0.5% - product_id=9999)
    num_orphan_products = 120
    orphan_product_indices = np.random.choice(order_items_mod.index, size=num_orphan_products, replace=False)
    order_items_mod.loc[orphan_product_indices, "product_id"] = 9999
    stats["orphan_order_item_product_ids"] = num_orphan_products

    return customers_mod, products_mod, orders_mod, order_items_mod, stats


# -----------------------------------------------------------------------------
# MAIN EXECUTION
# -----------------------------------------------------------------------------
def main() -> None:
    """Generate all raw e-commerce CSVs and introduce realistic data quality flaws."""
    print("=" * 70)
    print("Starting Synthetic E-Commerce Dataset Generation (Phase 1)")
    print("=" * 70)

    # 1. Ensure target directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Generate baseline synthetic data
    print("\n[1/4] Generating base datasets (seed=42)...")
    customers = generate_customers(num_customers=NUM_CUSTOMERS)
    products = generate_products(num_products=NUM_PRODUCTS)
    orders = generate_orders(customers_df=customers, num_orders=NUM_ORDERS)
    order_items = generate_order_items(orders_df=orders, products_df=products)

    # 3. Introduce controlled, realistic data quality flaws for validation demo
    print("[2/4] Introducing realistic data-quality flaws (0.5% - 1.0%)...")
    customers_raw, products_raw, orders_raw, order_items_raw, quality_stats = introduce_data_quality_issues(
        customers, products, orders, order_items
    )

    # 4. Save to CSV files
    print("[3/4] Writing raw CSV files to data/raw/...")
    files = {
        "customers": (OUTPUT_DIR / "customers.csv", customers_raw),
        "products": (OUTPUT_DIR / "products.csv", products_raw),
        "orders": (OUTPUT_DIR / "orders.csv", orders_raw),
        "order_items": (OUTPUT_DIR / "order_items.csv", order_items_raw),
    }

    for name, (path, df) in files.items():
        df.to_csv(path, index=False)
        print(f"  -> Saved {name:12} : {path.as_posix()} ({len(df):,} rows)")

    # 5. Print summary
    print("\n" + "=" * 70)
    print("DATASET GENERATION SUMMARY")
    print("=" * 70)
    print(f"Total Customers   : {len(customers_raw):,} rows (includes {quality_stats['duplicate_customers']} duplicates)")
    print(f"Total Products    : {len(products_raw):,} rows")
    print(f"Total Orders      : {len(orders_raw):,} rows")
    print(f"Total Order Items : {len(order_items_raw):,} rows")
    print("\nIntentionally Introduced Quality Issues:")
    for issue, count in quality_stats.items():
        print(f"  - {issue:32}: {count:,} records")
    print("=" * 70)
    print("Phase 1 completed successfully. Raw datasets ready for pipeline ingestion.")


if __name__ == "__main__":
    main()
