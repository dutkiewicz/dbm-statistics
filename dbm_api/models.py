from sqlalchemy import Column, String, Date, Integer, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BasicStats(Base):
    __tablename__ = 'dbm_basic_stats'

    # dimensions
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    advertiser = Column(String(255))
    advertiser_id = Column(Integer)
    insertion_order = Column(String(255))
    insertion_order_id = Column(Integer)
    line_item = Column(String(500))
    line_item_id = Column(Integer)
    currency = Column(String(5))

    # metrics
    impressions = Column(Integer)
    viewable_impressions = Column(Integer)
    clicks = Column(Integer)
    total_conversions = Column(Integer)
    post_click_conversions = Column(Integer)
    post_view_conversions = Column(Integer)
    total_cost = Column(Float(precision=6))
