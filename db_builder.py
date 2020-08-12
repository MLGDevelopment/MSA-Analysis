from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Table, Float
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class AxioProperty(Base):

    __tablename__ = "axio_properties"
    property_id = Column(String, primary_key=True)
    property_name = Column(String)
    property_address = Column(String)
    property_owner = Column(String)
    property_management = Column(String)
    year_built = Column(String)
    total_units = Column(Integer)
    total_square_feet = Column(Integer)
    rent_comp_data = relationship("RentComp")


class RentComp(Base):

    __tablename__ = 'rent_comps'

    id = Column(Integer, primary_key=True)
    date_added = Column(Date)
    property_id = Column(String, ForeignKey('axio_properties.property_id'))
    type = Column(String)
    area = Column(Integer)
    quantity = Column(Integer)
    avg_market_rent = Column(Integer)
    avg_effective_rent = Column(Integer)
    #property_occupancy = Column(Float)


engine = create_engine('postgres://postgres:admin@localhost:5432/Acquisitions')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

session = Session()

if __name__ == "__main__":
    pass
    # r_c = RentComp(property_id=10001, type="2B/2B", area=1000, quantity=50, avg_market_rent=1200, avg_effective_rent=1000)
    # session.add(r_c)
    # session.commit()