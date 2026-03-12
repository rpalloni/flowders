import random
from datetime import datetime
from pydantic import BaseModel


class Order(BaseModel):
    order_id: int
    customer_id: str
    amount: float
    timestamp: datetime


def generate_order(list_customer_id: list[str]) -> str:
    order = Order(
        order_id = random.randint(1000,9999),
        customer_id = random.choice(list_customer_id),
        amount = round(random.uniform(10.0,1000.0),2),
        timestamp = datetime.now()
    )
    return order.model_dump_json()