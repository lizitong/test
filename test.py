"""
VendorID			tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count		trip_distance
pickup_longitude	pickup_latitude			RateCodeID				store_and_fwd_flag	dropoff_longitude	
dropoff_latitude	payment_type			fare_amount				extra				mta_tax				
tip_amount			tolls_amount			improvement_surcharge	total_amount
"""
from pyspark import SparkConf, SparkContext, SparkFiles
from operator import add
#import shapefile

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
  		pass

	point1 = (round(x1, 2), round(y1, 2))
	point2 = (round(x2, 2), round(y2, 2))
	county1 = findCounty(point1)
	county2 = findCounty(point2)
	newLine = [time1, time2, county1, county2]
	#newLine = [time1, time2, point1, point2]
	#newLine = (time + "," + county, 1)
	return newLine

def findCounty(point):
	county = "Not found"
	idx = spatialIdx.value
	cs = couties.value
	try:
		county = cs[idx[point]]
	except:
		county = "Richmond"
	return county

def mapLocations(line):
	point = line[0]
	county = findCounty(point)
	if county == "Not found": county = "Richmond"
	newLine = (county, line[1])
	return newLine

def mapRoutes(line):
	trip = line[0]
	point1 = trip[1]
	point2 = trip[2]
	county1 = findCounty(point1)
	if county1 == "Not found": county1 = "Richmond"
	county2 = findCounty(point2)
	if county2 == "Not found": county2 = "Richmond"
	newLine = ((trip[0],county1,county2), line[1])
	return newLine


def deleteInvalidLines(line):
	try:
		int(line[0])
	except:
		return False
	return line[9] != "0.0" and line[10] != "0.0"

def readSpatialIndex(path):
	idx = {}
	f = open(path, "r")
	for line in f:
		line = line.split(",")
		point = (float(line[0]), float(line[1]))
		countyIndex = int(line[2])
		idx[point] = countyIndex
	f.close()
	return idx

def main(sc):

	csvPaths = []
	#csvPaths.append("/taxidata/green/")
	for i in range(1, 13):
		if i < 10:
			month = "0"+str(i)
		else: month = str(i)
		#green_tripdata_2015-01.csv
		path = "/taxidata/green/green_tripdata_2015-" + month + ".csv"
		csvPaths.append(path)
		#path2 = "/taxidata/yellow/yellow_tripdata_2015-" + month + ".csv"
		#csvPaths.append(path2)

	totalTimes = sc.emptyRDD()
	totalLocations = sc.emptyRDD()
	totalRoutes = sc.emptyRDD()
	for path in csvPaths:
		trips = sc.textFile(path).map(lambda line:line.split(",")).filter(deleteInvalidLines)
		parsedTrips = trips.map(parse)
		parsedTrips.cache()
		busyTimes = parsedTrips.flatMap(lambda line:line[0:2]).map(lambda x:(x,1)).reduceByKey(add)
		busyLocations = parsedTrips.flatMap(lambda line:line[2:4]).map(lambda x:(x,1)).reduceByKey(add)
		busyRoutes = parsedTrips.map(lambda line:((line[0],line[2],line[3]), 1)).reduceByKey(add)
		parsedTrips.unpersist()
		"""
		busyTimes = parsedTrips.flatMap(lambda line:line[0:2]).map(lambda x:(x,1)).reduceByKey(add)
		locations = parsedTrips.flatMap(lambda line:line[2:4]).map(lambda x:(x,1)).reduceByKey(add)
		busyLocations = locations.map(mapLocations).reduceByKey(add)
		routes = parsedTrips.map(lambda line:((line[0],line[2],line[3]), 1)).reduceByKey(add)
		busyRoutes = routes.map(mapRoutes).reduceByKey(add)
		parsedTrips.unpersist()
		"""

		totalTimes = totalTimes.union(busyTimes)
		totalLocations = totalLocations.union(busyLocations)
		totalRoutes = totalRoutes.union(busyRoutes)

	totalTimes.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("/zitong/output/Busy_Times")
	totalLocations.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("/zitong/output/Busy_Locations")
	totalRoutes.reduceByKey(add).sortBy(lambda x:x[1], False).saveAsTextFile("/zitong/output/Busy_Routes")

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAppName(APP_NAME)
	conf.setMaster('yarn-client')
	conf.set('spark.executor.memory', '1g')
    conf.set('spark.executor.cores','4')
    conf.set('spark.executor.instances','7')
	sc = SparkContext(conf=conf)
	sc.addPyFile("shapefile.py")
	COUNTIES = ['Albany', 'Allegany', 'Bronx', 'Broome', 'Cattaraugus', 'Cayuga', 
			'Chautauqua', 'Chemung', 'Chenango', 'Clinton', 'Columbia', 'Cortland', 
			'Delaware', 'Dutchess', 'Erie', 'Essex', 'Franklin', 'Fulton', 'Genesee', 
			'Greene', 'Hamilton', 'Herkimer', 'Jefferson', 'Kings', 'Lewis', 
			'Livingston', 'Madison', 'Monroe', 'Montgomery', 'Nassau', 'New York', 
			'Niagara', 'Oneida', 'Onondaga', 'Ontario', 'Orange', 'Orleans', 'Oswego', 
			'Otsego', 'Putnam', 'Queens', 'Rensselaer', 'Richmond', 'Rockland', 'Saratoga', 
			'Schenectady', 'Schoharie', 'Schuyler', 'Seneca', 'St. Lawrence', 'Steuben', 'Suffolk', 
			'Sullivan', 'Tioga', 'Tompkins', 'Ulster', 'Warren', 'Washington', 'Wayne', 
			'Westchester', 'Wyoming', 'Yates']
	spatialIdx = readSpatialIndex("spatialIdx.csv")
	spatialIdx = sc.broadcast(spatialIdx)
	couties = sc.broadcast(COUNTIES)
	main(sc)
