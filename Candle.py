from datetime import datetime
# ****************************************************************************************


class Candle:

    # Constructor
    def __init__(self, Id: int, Index: int, Token: int,  Date: datetime, Open: float, High: float, Low: float, Close: float, High_Low: str):

        # Validating the Input Data
        assert Open >= 0, f"Open price: {Open} is less than or equal to  zero"
        assert High >= 0, f"Open price: {High} is less than or equal to  zero"
        assert Low >= 0, f"Open price: {Low} is less than or equal to  zero"
        assert Close >= 0, f"Open price: {Close} is less than or equal to  zero"
        assert Index >= 0, f"Candle index: {Index} is less than or equal to  zero"

        # Assignment
        self.Date = datetime.strftime(Date, "%Y-%m-%d %H:%M")
        self.Open = Open
        self.High = High
        self.Low = Low
        self.Close = Close
        self.Index = Index
        self.High_Low = High_Low
        self.Id = Id
        self.Token = Token
