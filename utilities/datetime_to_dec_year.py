"""
Convert datetime to a decimal year.

Code found at: https://stackoverflow.com/questions/6451655/python-how-to-convert-datetime-dates-to-decimal-years

"""

from datetime import datetime as dt
import time

def toYearFraction(date):

    def sinceEpoch(date): # returns seconds since epoch
        return time.mktime(date.timetuple())

    s = sinceEpoch

    year = date.year

    startOfThisYear = dt(year = year, month = 1, day = 1)
    startOfNextYear = dt(year = year + 1, month = 1, day = 1)

    yearElapsed = s(date) - s(startOfThisYear)
    
    yearDuration = s(startOfNextYear) - s(startOfThisYear)
    
    fraction = yearElapsed/yearDuration

    return date.year + fraction


def datetime_to_dec_year(datetime_value, decimal_places):

    fractional_year = toYearFraction(datetime_value)

    return round(fractional_year, decimal_places)




