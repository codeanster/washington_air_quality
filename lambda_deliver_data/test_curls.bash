curl -X GET "https://6tczakegoh.execute-api.us-west-1.amazonaws.com/Prod/api/v1/air_quality/above_avg_locations" \
-H "Content-Type: application/json"

curl -X GET "https://6tczakegoh.execute-api.us-west-1.amazonaws.com/Prod/api/v1/air_quality/get_air_quality_trend/Spokane?timeframe=week" \
-H "Content-Type: application/json"

curl -X GET "https://6tczakegoh.execute-api.us-west-1.amazonaws.com/Prod/api/v1/air_quality/current/Spokane" \
-H "Content-Type: application/json"
