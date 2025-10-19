import datetime
import random
import os

import pandas as pd
from faker import Faker


def gaussian_clamped(rng: random.Random, mu: float, sigma: float, a: float, b: float) -> float:
    # Box-Muller with clamp
    val = rng.gauss(mu, sigma)
    return max(a, min(b, val))


def generate_orders(
    orders: int,
    seed: int = 42,
    start: str = "2024-01-01",
    end: str = "2025-09-01",
    inventory: pd.DataFrame = None,
    customers: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Generate orders data and return as a pandas DataFrame.

    Args:
        orders: Number of orders to generate
        seed: Random seed for reproducibility
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        inventory: Path to inventory_data.csv (optional, improves realism)
        customers: Path to customers.csv (optional, improves realism)

    Returns:
        pandas.DataFrame: DataFrame containing orders data
    """
    rng = random.Random(seed)

    start_dt = datetime.datetime.fromisoformat(start).replace(tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime.fromisoformat(end).replace(tzinfo=datetime.timezone.utc)
    delta_seconds = int((end_dt - start_dt).total_seconds())

    # Load lookups if available
    product_ids = list(range(1000, 1250))
    prices = {}
    if inventory is not None and not inventory.empty:
        product_ids = inventory["product_id"].tolist()
        prices = dict(zip(inventory["product_id"], inventory["unit_price"], strict=True))

    customer_ids = list(range(1, 1001))
    if customers is not None and not customers.empty:
        customer_ids = customers["customer_id"].tolist()

    rows = []
    for i in range(1, orders + 1):
        pid = rng.choice(product_ids)
        qty = rng.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.12, 0.06, 0.02])[0]
        sold_at = start_dt + datetime.timedelta(seconds=rng.randint(0, delta_seconds))
        cid = rng.choice(customer_ids)
        unit_price = prices.get(pid, round(rng.uniform(5, 500), 2))
        order_total = qty * unit_price
        rows.append(
            {
                "order_id": i,
                "product_id": pid,
                "customer_id": cid,
                "quantity": qty,
                "unit_price": unit_price,
                "order_total": order_total,
                "sold_at": sold_at,
            }
        )

    df = pd.DataFrame(rows)

    # Ensure SOLD_AT is properly formatted as datetime
    df["sold_at"] = pd.to_datetime(df["sold_at"])

    print(f"✅ Generated {len(df)} orders as DataFrame")
    return df


CATEGORIES = [
    ("Beauty", ["High", "Medium", "Low"]),
    ("Personal Care", ["High", "Medium", "Low", "Premium"]),
    ("Health", ["Male", "Female", "Unisex"]),
    ("Skincare", ["High", "Medium", "Low"]),
    ("Hair Care", ["High", "Medium", "Low"]),
]

ADJECTIVES = ["Classic", "Premium", "Eco", "Urban", "Sport", "Comfort", "Pro", "Lite", "Max", "Essential"]


def generate_inventory_data(products: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate inventory data and return as a pandas DataFrame.

    Args:
        products: Number of products to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing inventory data
    """
    rng = random.Random(seed)

    rows = []
    product_ids = list(range(1000, 1000 + products))
    for pid in product_ids:
        cat, names = rng.choice(CATEGORIES)
        base = rng.choice(names)
        adj = rng.choice(ADJECTIVES)
        product_name = f"{adj} {base}"
        # Base price by category with some variance
        base_price = {"Beauty": 25, "Personal Care": 15, "Health": 35, "Skincare": 30, "Hair Care": 20}.get(cat, 20)
        price = round(gaussian_clamped(rng, base_price, base_price * 0.25, base_price * 0.4, base_price * 1.8), 2)
        # Stock skewed: long tail
        stock_qty = int(gaussian_clamped(rng, 80, 60, 0, 400))
        rows.append(
            {
                "product_id": pid,
                "product_name": product_name,
                "category": cat,
                "unit_price": price,
                "stock_quantity": stock_qty,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} inventory data as DataFrame")
    return df


# -- Generate customers --


def generate_customers(customers: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate customer data and return as a pandas DataFrame.

    Args:
        customers: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        pandas.DataFrame: DataFrame containing customer data
    """
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    rows = []
    channels = [("online", 0.65), ("store", 0.35)]

    for cid in range(1, customers + 1):
        name = fake.name()
        email = fake.email()
        city = fake.city()
        channel = rng.choices([c for c, _ in channels], weights=[w for _, w in channels])[0]
        created_at = fake.date_time_between(start_date='-2y', end_date='now', tzinfo=datetime.timezone.utc)
        rows.append(
            {
                "customer_id": cid,
                "name": name,
                "email": email,
                "city": city,
                "channel": channel,
                "created_at": created_at,
            }
        )

    df = pd.DataFrame(rows)
    print(f"✅ Generated {len(df)} customers as DataFrame")
    return df


if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Generate data
    customers_df = generate_customers(customers=100, seed=42)
    inventory_df = generate_inventory_data(products=100, seed=42)
    orders_df = generate_orders(orders=100, seed=42, inventory=inventory_df, customers=customers_df)
    
    # Save to CSV files
    customers_df.to_csv("data/customers.csv", index=False)
    inventory_df.to_csv("data/products.csv", index=False)
    orders_df.to_csv("data/orders.csv", index=False)
    
    print("✅ All data files generated successfully!")

