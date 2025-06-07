// CREATING NODES AND EDGES

/* Nodes :Airport */
// Create constraints
CREATE CONSTRAINT ON (n:`Airport`) ASSERT EXISTS (n.`code`);
CREATE CONSTRAINT ON (n:`Airport`) ASSERT n.`code` IS UNIQUE;
CREATE CONSTRAINT ON (n:`Airport`) ASSERT EXISTS (n.`city`);
CREATE CONSTRAINT ON (n:`Airport`) ASSERT EXISTS (n.`country`);

// Import nodes
LOAD CSV FROM $file WITH HEADER IGNORE BAD AS row
  CREATE (:`Airport` {
    `code`: CASE row.`code` WHEN "" THEN null ELSE row.`code` END,
    `city`: CASE row.`city` WHEN "" THEN null ELSE row.`city` END,
    `country`: CASE row.`country` WHEN "" THEN null ELSE row.`country` END
  });


/* Nodes :City */ 
// Create constraints
CREATE CONSTRAINT ON (n:`City`) ASSERT EXISTS (n.`name`);
CREATE CONSTRAINT ON (n:`City`) ASSERT n.`name` IS UNIQUE;
CREATE CONSTRAINT ON (n:`City`) ASSERT EXISTS (n.`population`);
CREATE CONSTRAINT ON (n:`City`) ASSERT EXISTS (n.`country`);

// Import nodes
LOAD CSV FROM $file WITH HEADER IGNORE BAD AS row
  CREATE (:`City` {
    `name`: CASE row.`name` WHEN "" THEN null ELSE row.`name` END,
    `population`: toInteger(row.`population`),
    `country`: CASE row.`country` WHEN "" THEN null ELSE row.`country` END
  });

/* Edges :NEAREST_AIRPORT */
// Import relationships 
LOAD CSV FROM $file WITH HEADER IGNORE BAD AS row
  MATCH (n:`City` {
    `name`: CASE row.`name` WHEN "" THEN null ELSE row.`name` END
  })
  MATCH (m:`Airport` {
    `name`: CASE row.`nearestAirportCode` WHEN "" THEN null ELSE row.`nearestAirportCode` END
  })
  CREATE (n)-[:`NEAREST_AIRPORT` {
    `kmDistanceToNearestAirportCity: toFloat(row.`kmDistanceToNearestAirportCity`) 
  }]->(m);


/* Edges :FLIGHT_TO */
LOAD CSV FROM $file WITH HEADER IGNORE BAD AS row
WITH
    row,
    row.departureAirport AS depCode,
    row.destinationAirport AS destCode
WHERE row.price IS NOT NULL AND row.price <> "Price unavailable"
MATCH (n:Airport { code: depCode })
MATCH (m:Airport { code: destCode })
CREATE (n)-[:FLIGHT_TO {
    company: row.company,
    departureTimestamp: localDateTime(row.departureTimestamp),
    arrivalTimestamp: localDateTime(row.arrivalTimestamp),
    durationMinutes: toInteger(row.duration),
    priceEur: toFloat(row.price)
}]->(m);


// INTERESTING QUERIES

/* 
 * Query Purpose: Lists all cities in the database by extracting city names from both City nodes 
 * and Airport nodes.
 *
 * Explanation:
 * - Matches all nodes in the database without filtering
 * - Uses a CASE statement to extract city names based on node type:
 *   - From City nodes: uses the name property
 *   - From Airport nodes: uses the city property
 *   - Ignores other node types (returns NULL)
 * 
 * Application Context:
 * Provides the initial list of cities for users to select as origin/destination in the 
 * flight search application interface.
 */
MATCH (n)
RETURN CASE 
          WHEN n:City THEN n.name 
          WHEN n:Airport THEN n.city
          ELSE NULL 
       END AS result;
//--------------------------------------------------------------------------------------------------------------------------------------

/* 
 * Query Purpose: Finds the appropriate airport codes for departure and destination cities, 
 * handling cases where cities have their own airports or need to use their nearest airport.
 *
 * Explanation:
 * - Starts with hardcoded city names ("Cakovec" and "Versailles") that would typically come from user input
 * - For departure city:
 *   - Matches either a City node with matching name or an Airport node where city property matches
 *   - If it's a City, follows NEAREST_AIRPORT relationship to find nearest airport
 * - Similar process for destination city
 * - Uses CASE statements to determine final airport codes:
 *   - If departure/destination node is an Airport, uses its code directly
 *   - Otherwise uses the code from the nearest airport found via relationship
 * 
 * Key Components:
 * - WITH statement: Initializes city names for departure and destination
 * - Dual WHERE conditions: Uses OR to match either City nodes by name or Airport nodes by city property
 * - OPTIONAL MATCH: Ensures query still works even if nearest airport relationships don't exist
 * - Multiple WITH clauses: Carries necessary variables between query parts while managing scope
 * - CASE statements: Implements logic to determine the correct airport code based on node type
 * - Pattern: (departureNode:City)-[rDep:NEAREST_AIRPORT]->(nearestDepartureAirport:Airport) captures
 *   the relationship between cities and their nearest airports
 * 
 * Application Context:
 * This serves as a critical subquery for the flight search application, translating user-selected 
 * city names into operational airport codes, which are necessary for all subsequent flight 
 * searches. It handles the common scenario where users want to travel to/from cities that may 
 * not have their own airport.
 */

WITH "Cakovec" AS departureCity, "Versailles" AS destinationCity

MATCH (departureNode)
WHERE (departureNode:City AND departureNode.name = departureCity) OR (departureNode:Airport AND departureNode.city = departureCity)
OPTIONAL MATCH (departureNode:City)-[rDep:NEAREST_AIRPORT]->(nearestDepartureAirport:Airport)
WITH departureCity, destinationCity, 
     departureNode, nearestDepartureAirport, rDep
MATCH (destinationNode)
WHERE (destinationNode:City AND destinationNode.name = destinationCity) OR (destinationNode:Airport AND destinationNode.city = destinationCity)
OPTIONAL MATCH (destinationNode:City)-[rDest:NEAREST_AIRPORT]->(nearestDestinationAirport:Airport)
WITH 
    CASE
        WHEN departureNode:Airport THEN departureNode.code
        ELSE nearestDepartureAirport.code
    END AS departureAirportCode,
    CASE
        WHEN destinationNode:Airport THEN destinationNode.code
        ELSE nearestDestinationAirport.code
    END AS destinationAirportCode,
    departureNode,
    destinationNode,
    nearestDepartureAirport,
    nearestDestinationAirport,
    rDep,
    rDest

RETURN departureAirportCode, destinationAirportCode;

//--------------------------------------------------------------------------------------------------------------------------------------

/* 
 * Query Purpose: Finds the cheapest direct flight between two cities (Cakovec and Eisenstadt),
 * including connection details for cities that don't have their own airports.
 *
 * Explanation:
 * - First uses the airport-finding subquery to identify appropriate airport codes for both cities
 * - Then matches a direct flight path between the identified airports
 * - Returns comprehensive information about:
 *   - The departure and destination nodes (cities or airports)
 *   - Paths from cities to their nearest airports (if applicable)
 *   - The direct flight details (nodes, relationship, duration, price, timestamps)
 * - Orders results by price to find the cheapest option
 * - Limits to just the single cheapest flight
 * 
 * Key Components:
 * - MATCH path=(...): Captures the entire flight path as a variable for easy processing
 * - {code: departureAirportCode}: Uses the airport codes determined in the subquery
 * - CASE WHEN statements in RETURN: Conditionally returns path details only when cities use nearest airports
 * - ORDER BY r.priceEur ASC: Sorts results by price in ascending order
 * - LIMIT 1: Returns only the cheapest flight
 * 
 * Application Context:
 * This query implements a common flight search scenario where users want to find the cheapest 
 * direct flight between two cities, similar to the "nonstop" filter combined with "sort by price" 
 * in commercial flight search applications.
 */

WITH "Cakovec" AS departureCity, "Eisenstadt" AS destinationCity

MATCH (departureNode)
WHERE (departureNode:City AND departureNode.name = departureCity) OR (departureNode:Airport AND departureNode.city = departureCity)
OPTIONAL MATCH (departureNode:City)-[rDep:NEAREST_AIRPORT]->(nearestDepartureAirport:Airport)
WITH departureCity, destinationCity, 
     departureNode, nearestDepartureAirport, rDep
MATCH (destinationNode)
WHERE (destinationNode:City AND destinationNode.name = destinationCity) OR (destinationNode:Airport AND destinationNode.city = destinationCity)
OPTIONAL MATCH (destinationNode:City)-[rDest:NEAREST_AIRPORT]->(nearestDestinationAirport:Airport)
WITH 
    CASE
        WHEN departureNode:Airport THEN departureNode.code
        ELSE nearestDepartureAirport.code
    END AS departureAirportCode,
    CASE
        WHEN destinationNode:Airport THEN destinationNode.code
        ELSE nearestDestinationAirport.code
    END AS destinationAirportCode,
    departureNode,
    destinationNode,
    nearestDepartureAirport,
    nearestDestinationAirport,
    rDep,
    rDest

// Use the airport codes from the first part - CHEAPEST DIRECT FLIGHT
MATCH path=(n {code: departureAirportCode})-[r:FLIGHT_TO]->(m {code: destinationAirportCode})
RETURN 
    departureNode,
    CASE WHEN rDep IS NOT NULL THEN [departureNode, rDep, nearestDepartureAirport] ELSE [] END AS departureToAirportPath,
    destinationNode,
    CASE WHEN rDest IS NOT NULL THEN [destinationNode, rDest, nearestDestinationAirport] ELSE [] END AS destinationToAirportPath,
    nodes(path) AS directFlightNodes, 
    relationships(path) AS directFlightRelationship, 
    r.durationMinutes AS flightDuration,
    r.priceEur AS flightPrice,
    r.departureTimestamp AS departureTime,
    r.arrivalTimestamp AS arrivalTime
ORDER BY r.priceEur ASC
LIMIT 1;

//--------------------------------------------------------------------------------------------------------------------------------------

/* 
 * Query Purpose: Finds the cheapest flight route between two cities (Cakovec and Eisenstadt), 
 * allowing for unlimited connections while ensuring the total travel time doesn't exceed 2 days.
 *
 * Explanation:
 * - Uses the airport-finding subquery to determine appropriate airport codes
 * - Implements a weighted shortest path algorithm to find the cheapest route
 * - Now allows for any number of connections
 * - Applies constraints to ensure realistic connections:
 *   - For connecting flights, arrival of previous flight must be before departure of next flight
 *   - Total travel time must not exceed 2 days (based on day comparison)
 * 
 * Key Components:
 * - [:FLIGHT_TO *WSHORTEST (r, n | r.priceEur) totalPrice]: Uses weighted shortest path algorithm
 *   with flight price as the weight to minimize
 * - Complex path filter with multiple conditions:
 *   - size(relationships(p)) <= 1 or...: Either direct flight or proper connection timing
 *   - ((relationships(p)[-2]).arrivalTimestamp < r.departureTimestamp): Ensures arrival at
 *     connection airport happens before departure of next flight
 *   - (relationships(p)[0]).arrivalTimestamp.day + 2 > r.departureTimestamp.day: Limits total
 *     travel time to maximum 2 days
 * - totalPrice: Accumulates the total price across all flights in the path
 * 
 * Application Context:
 * This implements a more flexible flight search where users prioritize finding the absolute cheapest
 * option regardless of the number of connections, as long as the total journey time remains under
 * 2 days. This reflects real-world scenarios where budget-conscious travelers may accept multiple
 * layovers to achieve significant cost savings.
 */

WITH "Cakovec" AS departureCity, "Eisenstadt" AS destinationCity

MATCH (departureNode)
WHERE (departureNode:City AND departureNode.name = departureCity) OR (departureNode:Airport AND departureNode.city = departureCity)
OPTIONAL MATCH (departureNode:City)-[rDep:NEAREST_AIRPORT]->(nearestDepartureAirport:Airport)
WITH departureCity, destinationCity, 
     departureNode, nearestDepartureAirport, rDep
MATCH (destinationNode)
WHERE (destinationNode:City AND destinationNode.name = destinationCity) OR (destinationNode:Airport AND destinationNode.city = destinationCity)
OPTIONAL MATCH (destinationNode:City)-[rDest:NEAREST_AIRPORT]->(nearestDestinationAirport:Airport)
WITH 
    CASE
        WHEN departureNode:Airport THEN departureNode.code
        ELSE nearestDepartureAirport.code
    END AS departureAirportCode,
    CASE
        WHEN destinationNode:Airport THEN destinationNode.code
        ELSE nearestDestinationAirport.code
    END AS destinationAirportCode,
    departureNode,
    destinationNode,
    nearestDepartureAirport,
    nearestDestinationAirport,
    rDep,
    rDest

// CHEAPEST FLIGHT WITH MULTIPLE LAYOVERS BUT WITH MAX OF 2 DAYS TRAVELLING
// Use the airport codes from the first part in the second part
MATCH path=(n {code: departureAirportCode})-[:FLIGHT_TO *WSHORTEST (r, n | r.priceEur) totalPrice 
  (r, n, p, w | 
    size(relationships(p)) <= 1 or 
    ((relationships(p)[-2]).arrivalTimestamp < r.departureTimestamp) and 
    (relationships(p)[0]).arrivalTimestamp.day + 2 > r.departureTimestamp.day
  )]->(m {code: destinationAirportCode})
RETURN 
    departureNode,
    CASE WHEN rDep IS NOT NULL THEN [departureNode, rDep, nearestDepartureAirport] ELSE [] END AS departureToAirportPath,
    destinationNode,
    CASE WHEN rDest IS NOT NULL THEN [destinationNode, rDest, nearestDestinationAirport] ELSE [] END AS destinationToAirportPath,
    nodes(path) AS shortestPathNodes, 
    relationships(path) AS shortestPathRelationships, 
    totalPrice;

//--------------------------------------------------------------------------------------------------------------------------------------

/* 
 * Query Purpose: Finds the fastest flight route between Zagreb and Porto by minimizing total flight 
 * duration, allowing for connections while ensuring the travel time doesn't exceed 1 day.
 *
 * Explanation:
 * - Uses the airport-finding subquery to determine appropriate airport codes
 * - Implements a weighted shortest path algorithm to find the route with minimum total flight duration
 * - Allows for multiple connections as long as they meet the timing constraints (1-day travel time)
 * 
 * Key Components:
 * - [:FLIGHT_TO *WSHORTEST (r, n | r.durationMinutes) totalDuration]: Uses weighted shortest path 
 *   algorithm with flight duration as the weight to minimize
 * - Path filter conditions:
 *   - size(relationships(p)) <= 1 or...: Either direct flight or proper connection timing
 *   - ((relationships(p)[-2]).arrivalTimestamp < r.departureTimestamp): Ensures arrival at
 *     connection airport happens before departure of next flight
 *   - (relationships(p)[0]).arrivalTimestamp.day + 1 > r.departureTimestamp.day: Limits total
 *     travel time to maximum 1 day (corrected from previous +2 which allowed up to 2 days)
 * - totalDuration: Accumulates the total flight duration in minutes across all segments
 * 
 * Application Context:
 * This implements a flight search optimized for travelers who prioritize minimizing travel time
 * with a strict constraint of completing the journey within a single day. This is particularly
 * useful for business travelers on tight schedules who need to arrive at their destination
 * within the same day or next morning after departure.
 */
 
WITH "Zagreb" AS departureCity, "Porto" AS destinationCity

MATCH (departureNode)
WHERE (departureNode:City AND departureNode.name = departureCity) OR (departureNode:Airport AND departureNode.city = departureCity)
OPTIONAL MATCH (departureNode:City)-[rDep:NEAREST_AIRPORT]->(nearestDepartureAirport:Airport)
WITH departureCity, destinationCity, 
     departureNode, nearestDepartureAirport, rDep
MATCH (destinationNode)
WHERE (destinationNode:City AND destinationNode.name = destinationCity) OR (destinationNode:Airport AND destinationNode.city = destinationCity)
OPTIONAL MATCH (destinationNode:City)-[rDest:NEAREST_AIRPORT]->(nearestDestinationAirport:Airport)
WITH 
    CASE
        WHEN departureNode:Airport THEN departureNode.code
        ELSE nearestDepartureAirport.code
    END AS departureAirportCode,
    CASE
        WHEN destinationNode:Airport THEN destinationNode.code
        ELSE nearestDestinationAirport.code
    END AS destinationAirportCode,
    departureNode,
    destinationNode,
    nearestDepartureAirport,
    nearestDestinationAirport,
    rDep,
    rDest

// Use the airport codes from the first part in the second part
MATCH path=(n {code: departureAirportCode})-[:FLIGHT_TO *WSHORTEST (r, n | r.durationMinutes) totalDuration 
  (r, n, p, w | 
    size(relationships(p)) <= 1 or 
    ((relationships(p)[-2]).arrivalTimestamp < r.departureTimestamp) and 
    (relationships(p)[0]).arrivalTimestamp.day + 1 > r.departureTimestamp.day
  )]->(m {code: destinationAirportCode})
RETURN 
    departureNode,
    CASE WHEN rDep IS NOT NULL THEN [departureNode, rDep, nearestDepartureAirport] ELSE [] END AS departureToAirportPath,
    destinationNode,
    CASE WHEN rDest IS NOT NULL THEN [destinationNode, rDest, nearestDestinationAirport] ELSE [] END AS destinationToAirportPath,
    nodes(path) AS shortestPathNodes, 
    relationships(path) AS shortestPathRelationships, 
    totalDuration;