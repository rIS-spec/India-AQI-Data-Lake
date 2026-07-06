from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


# define a schema before writing the fetching code to read the data from the API 
# means This is the INPUT to the model 
class AQIRecord(BaseModel):
    city: str
    state: str
    aqi: float
    timestamp: datetime
    station: Optional[str] = None
    pm25: Optional[float] = None
    pm10: Optional[float] = None
    no2: Optional[float] = None
    so2: Optional[float] = None

    aqi_category: Optional[str] = None

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}



