"""
VendorID			tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count		trip_distance
pickup_longitude	pickup_latitude			RateCodeID				store_and_fwd_flag	dropoff_longitude	
dropoff_latitude	payment_type			fare_amount				extra				mta_tax				
tip_amount			tolls_amount			improvement_surcharge	total_amount
"""
from pyspark import SparkConf, SparkContext, SparkFiles
from operator import add
#from shapely.geometry import MultiPoint, Point, Polygon
import shapefile

APP_NAME = "Traffic_Peak_Analysis"

# determine if a point is inside a given polygon or not
# Polygon is a list of (x,y) pairs.
def point_inside_polygon(x,y,poly,bbox):
	maxX = max(bbox[0], bbox[2])
	minX = min(bbox[0], bbox[2])
	maxY = max(bbox[1], bbox[3])
	minY = min(bbox[1], bbox[3])
	if x < minX or x > maxX or y < minY or y > maxY:
		return False

	n = len(poly)
	inside = False
	p1x,p1y = poly[0]
	for i in range(n+1):
		p2x,p2y = poly[i % n]
		if y > min(p1y,p2y):
			if y <= max(p1y,p2y):
				if x <= max(p1x,p2x):
					if p1y != p2y:
						xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
					if p1x == p2x or x <= xinters:
						inside = not inside
		p1x,p1y = p2x,p2y

	return inside

#15/1/2015 19:23
#Only use hour and minute, converte minute to intervals of 10.
def parse(line):
	"""
	tpep_pickup_datetime	1
	tpep_dropoff_datetime	2
	pickup_longitude		5
	pickup_latitude			6
	dropoff_longitude		9
	dropoff_latitude 		10
	"""
	datetime1 = line[1].split()
	datetime2 = line[2].split()
	try:
		time1 = datetime1[1]
		time1 = time1.split(":")
		hour1 = time1[0]
		minute1 = int(time1[1])
		if minute1 < 30: minute1 = "00"
		else: minute1 = "30"
		time1 = hour1 + ":" + minute1
		x1 = float(line[5])
		y1 = float(line[6])

		time2 = datetime2[1]
		time2 = time2.split(":")
		hour2 = time2[0]
		minute2 = int(time2[1])
		if minute2 < 30: minute2 = "00"
		else: minute2 = "30"
		time2 = hour2 + ":" + minute2
		x2 = float(line[9])
		y2 = float(line[10])
  	except:
  		print "*********************"
  		print "Invalid Point, line is:", line
  		return ("Invalid Point", 1)
	county1 = "Not found"
	county2 = "Not found"
	srs = shapeRecs.value
	for sr in srs:
		coords = sr.shape.points
		bbox = sr.shape.bbox
		if point_inside_polygon(x1,y1,coords, bbox):
			county1 = sr.record[6]
		if point_inside_polygon(x2,y2,coords, bbox):
			county2 = sr.record[6]
		
	if county1 == "Not found":
		county1 = "Richmond"
	if county2 == "Not found":
		county2 = "Richmond"
	newLine = [time1, time2, county1, county2]
	#newLine = (time + "," + county, 1)
	return newLine

def deleteInvalidLines(line):
	try:
		int(line[0])
	except:
		return False
	return line[9] != "0.0" and line[10] != "0.0"

def main(sc):
	trips = sc.textFile("/taxidata/green/").map(lambda line:line.split(",")).filter(deleteInvalidLines)
	parsedTrips = trips.map(parse)
	parsedTrips.cache()
	busyLocations = parsedTrips.flatMap(lambda line:line[2:4]).map(lambda x:(x,1))
	bysyTimes = parsedTrips.flatMap(lambda line:line[0:2]).map(lambda x:(x,1))
	busyRoutes = parsedTrips.map(lambda line:(line[0]+","+line[2]+","+line[3], 1))
	busyLocations.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("Busy_Locations")
	bysyTimes.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("Busy_Times")
	busyRoutes.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("Busy_Trips")

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAppName(APP_NAME)
	conf.setMaster('yarn-client')
	sc = SparkContext(conf=conf)
	sc.addPyFile("shapefile.py")
	shapefilePath = "../NY_counties_clip/NY_counties_clip"
	sf = shapefile.Reader(shapefilePath)
	shapeRecords = sf.shapeRecords()
	shapeRecs = sc.broadcast(shapeRecords)
	main(sc)