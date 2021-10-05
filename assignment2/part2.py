'''
1.
SELECT COUNT(*) as "Number of Users"
FROM Users;

SELECT COUNT(*) as "Number of Activities"
FROM Activities;

SELECT COUNT(*) as "Number of Trackpoints"
FROM Trackpoints;

2.
SELECT COUNT(*) as NumberOfActivities, user_id
FROM Activities

GROUP BY (user_id)

SELECT AVG(NumberOfActivities), MAX(NumberOfActivities), Min(NumberOfActivities);

3.
SELECT COUNT(NumberOfActivities), user_id 
FROM Activities
ORDER BY COUNT(NumberOfActivities) DESC
LIMIT 10;

4.
'''
def dateFromDateTime(dateTime):
  return dateTime.split()[0] #YYYY-MM-DD HH:MM:SS becomes YYYY-MM-DD
'''

SELECT user_id, 
  dateFromDateTime(start_date_Time) as start_date, 
  dateFromDateTime(end_date_Time) as end_date
  COUNT(IF(start_date != end_date))
FROM Activities
GROUP BY user_id

5.

6.
'''
import math
def calculateDistance3D(x1,x2,y1,y2,z1,z2): # the space [x,y,z] where the plane [x,y] is [latitude,longitude], and z is altitude.
  return math.sqrt(math.pow(x2 - x1) + math.pow(y2 - y1) + math.pow(z2 - z1))

def isCloseInDistance(lat1, lat2, long1, long2, alt1, alt2):
  return calculateDistance3D(lat1, lat2, long1, long2, alt1, alt2) <= 100

def calculateTime(datetime1, datetime2):
  date1 = datetime1.split()[0]
  date2 = datetime2.split()[0]
  if date2 != date1:
    return

  time1 = datetime1.split()[1]
  time2 = datetime2.split()[1]

  seconds1 = time1.split(":")[0]*3600 + time1.split(":")[1] + time1.split(":")[0]
  seconds2 = time2.split(":")[0]*3600 + time2.split(":")[1] + time2.split(":")[0]

  return seconds2 - seconds1

def isCloseInTime(t1, t2):
  return calculateTime(t2, t1) <= 60
'''

SELECT 

'''