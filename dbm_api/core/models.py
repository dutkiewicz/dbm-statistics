from sqlalchemy import Column, String, Date, Integer, Numeric, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MetaNames(Base):
    """
    Basic table for storing names of campaigns, orders and line items
    for joining with other tables
    """
    __tablename__ = 'dbm_meta_names'

    id = Column(Integer, primary_key=True)

    advertiser_name = Column(String(250))
    advertiser_id = Column(Integer)

    order_name = Column(String(500))
    order_id = Column(Integer)

    line_item_name = Column(String(1000))
    line_item_id = Column(Integer)

    __table_args__ = (UniqueConstraint('line_item_id'), )


class BasicStats(Base):
    """
    Basic stats where dimension is only Line Item
    """
    __tablename__ = 'dbm_basic_stats'

    # dimensions
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    line_item_id = Column(Integer)
    currency = Column(String(5))

    # metrics
    impressions = Column(Integer)
    viewable_impressions = Column(Integer)
    clicks = Column(Integer)
    total_conversions = Column(Integer)
    post_click_conversions = Column(Integer)
    total_cost = Column(Numeric(12, 6))
    media_cost = Column(Numeric(12, 6))

    __table_args__ = (UniqueConstraint('date', 'line_item_id', name='unique_li_per_date'), )
