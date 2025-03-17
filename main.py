import os
import datetime
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# OAuth 2.0 scopes needed for Google Fit
SCOPES = [
    'https://www.googleapis.com/auth/fitness.activity.write',
    'https://www.googleapis.com/auth/fitness.activity.read'
]

# Your project number (not ID) extracted from client_id
# From your client_id: 394921715331-gp51o2vk0jcr0lbfv641kai7866k11od.apps.googleusercontent.com
PROJECT_NUMBER = "394921715331"

def get_credentials():
    """Get and refresh OAuth 2.0 credentials."""
    creds = None

    # The file token.json stores the user's access and refresh tokens
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_info(
            json.loads(open('token.json').read()))

    # If credentials don't exist or are invalid, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=54321)

        # Save credentials for future use
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds

def get_data_source_id(data_type_name):
    """Generate the proper data source ID according to Google Fit's requirements"""
    # Format: type:dataTypeName:projectNumber:deviceManufacturer:deviceModel:deviceUid:streamName
    return f"derived:{data_type_name}:{PROJECT_NUMBER}:microcloud:gfit-b100:1000001:{data_type_name}"

def create_data_source(fitness_service, data_type_name, application_name="GFit B100"):
    """Create a data source if it doesn't exist."""
    data_source_id = get_data_source_id(data_type_name)

    try:
        # First check if the data source already exists
        fitness_service.users().dataSources().get(
            userId='me',
            dataSourceId=data_source_id
        ).execute()
        print(f"Data source {data_source_id} already exists.")
        return data_source_id
    except HttpError as error:
        if error.resp.status == 404:
            # Create the data source with all required fields
            data_source = {
                "dataStreamId": data_source_id,
                "dataStreamName": data_type_name,
                "type": "derived",
                "application": {
                    "name": application_name,
                    "version": "1.0"
                },
                "dataType": {
                    "name": data_type_name,
                    "field": []
                },
                "device": {
                    "uid": "1000001",
                    "type": "unknown",
                    "version": "1.0",
                    "model": "gfit-b100",
                    "manufacturer": "microcloud"
                }
            }

            # Add specific fields based on data type
            if data_type_name == "com.google.activity.segment":
                data_source["dataType"]["field"] = [
                    {
                        "name": "activity",
                        "format": "integer"
                    }
                ]
            elif data_type_name == "com.google.calories.expended":
                data_source["dataType"]["field"] = [
                    {
                        "name": "calories",
                        "format": "floatPoint"
                    }
                ]
            elif data_type_name == "com.google.step_count.delta":
                data_source["dataType"]["field"] = [
                    {
                        "name": "steps",
                        "format": "integer"
                    }
                ]

            try:
                print(f"Creating data source: {data_source_id}")
                result = fitness_service.users().dataSources().create(
                    userId='me',
                    body=data_source
                ).execute()
                print(f"Data source created: {result.get('dataStreamId')}")
                return data_source_id
            except HttpError as create_error:
                print(f"Error creating data source: {create_error}")
                return None
        else:
            print(f"Error checking data source: {error}")
            return None

def log_activity(activity_type, start_time, end_time, calories=None, steps=None):
    """
    Log a fitness activity to Google Fit

    Args:
        activity_type: The activity type from
            https://developers.google.com/fit/rest/v1/reference/activity-types
        start_time: Start time as a datetime object
        end_time: End time as a datetime object
        calories: Optional calories burned (in kcal)
        steps: Optional step count for the activity
    """
    credentials = get_credentials()
    fitness_service = build('fitness', 'v1', credentials=credentials)

    # Create data sources if they don't exist
    activity_source_id = create_data_source(fitness_service, "com.google.activity.segment")
    calories_source_id = None
    steps_source_id = None

    if calories:
        calories_source_id = create_data_source(fitness_service, "com.google.calories.expended")

    if steps:
        steps_source_id = create_data_source(fitness_service, "com.google.step_count.delta")

    if not activity_source_id:
        print("Failed to create activity data source")
        return False

    # Convert times to nanoseconds
    start_time_ns = int(start_time.timestamp() * 1000000000)
    end_time_ns = int(end_time.timestamp() * 1000000000)

    # Create the dataset
    dataset_id = f"{start_time_ns}-{end_time_ns}"

    try:
        # Create the activity segment data points
        activity_segment = {
            'dataSourceId': activity_source_id,
            'minStartTimeNs': start_time_ns,
            'maxEndTimeNs': end_time_ns,
            'point': [{
                'startTimeNanos': start_time_ns,
                'endTimeNanos': end_time_ns,
                'dataTypeName': 'com.google.activity.segment',
                'value': [{
                    'intVal': activity_type
                }]
            }]
        }

        # Insert the activity data
        fitness_service.users().dataSources().datasets().patch(
            userId='me',
            dataSourceId=activity_source_id,
            datasetId=dataset_id,
            body=activity_segment
        ).execute()

        # If calories are provided, log them as well
        if calories and calories_source_id:
            calories_data = {
                'dataSourceId': calories_source_id,
                'minStartTimeNs': start_time_ns,
                'maxEndTimeNs': end_time_ns,
                'point': [{
                    'startTimeNanos': start_time_ns,
                    'endTimeNanos': end_time_ns,
                    'dataTypeName': 'com.google.calories.expended',
                    'value': [{
                        'fpVal': calories
                    }]
                }]
            }

            fitness_service.users().dataSources().datasets().patch(
                userId='me',
                dataSourceId=calories_source_id,
                datasetId=dataset_id,
                body=calories_data
            ).execute()

        # If steps are provided, log them as well
        if steps and steps_source_id:
            steps_data = {
                'dataSourceId': steps_source_id,
                'minStartTimeNs': start_time_ns,
                'maxEndTimeNs': end_time_ns,
                'point': [{
                    'startTimeNanos': start_time_ns,
                    'endTimeNanos': end_time_ns,
                    'dataTypeName': 'com.google.step_count.delta',
                    'value': [{
                        'intVal': steps
                    }]
                }]
            }

            fitness_service.users().dataSources().datasets().patch(
                userId='me',
                dataSourceId=steps_source_id,
                datasetId=dataset_id,
                body=steps_data
            ).execute()

        # Create a session
        log_session(fitness_service, activity_type, start_time, end_time)

        # Request aggregation for the inserted data
        if steps:
            request_data_aggregation(fitness_service, start_time, end_time)

        print("Data points inserted successfully")
        return True
    except HttpError as error:
        print(f"An error occurred: {error}")
        return False

def request_data_aggregation(fitness_service, start_time, end_time):
    """Request data aggregation to ensure steps are properly reflected"""
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)

    try:
        # Request aggregation for step data
        aggregation_request = {
            "aggregateBy": [{
                "dataTypeName": "com.google.step_count.delta"
            }],
            "bucketByTime": {"durationMillis": 86400000},  # 1 day in milliseconds
            "startTimeMillis": start_time_ms,
            "endTimeMillis": end_time_ms
        }

        fitness_service.users().dataset().aggregate(
            userId="me",
            body=aggregation_request
        ).execute()

        print("Data aggregation requested successfully")
        return True
    except HttpError as error:
        print(f"Error requesting data aggregation: {error}")
        return False

def log_session(fitness_service, activity_type, start_time, end_time):
    """Create a session for the activity"""
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)

    session = {
        "id": f"session-{start_time_ms}",
        "name": "GFit B100 Session",
        "description": f"Activity recorded via GFit B100",
        "startTimeMillis": start_time_ms,
        "endTimeMillis": end_time_ms,
        "application": {
            "name": "GFit B100"
        },
        "activityType": activity_type
    }

    try:
        fitness_service.users().sessions().update(
            userId='me',
            sessionId=f"session-{start_time_ms}",
            body=session
        ).execute()
        print(f"Session created successfully")
        return True
    except HttpError as error:
        print(f"Error creating session: {error}")
        return False

def clean_up_todays_activities():
    """Removes all fitness activities recorded for the current day."""
    credentials = get_credentials()
    fitness_service = build('fitness', 'v1', credentials=credentials)

    # Get today's start and end in milliseconds (for sessions) and nanoseconds (for datasets)
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + datetime.timedelta(days=1)

    # For datasets (nanoseconds)
    start_time_ns = int(today.timestamp() * 1000000000)
    end_time_ns = int(tomorrow.timestamp() * 1000000000)

    # Format dates in RFC3339 format for sessions API
    start_time_rfc3339 = today.isoformat() + "Z"
    end_time_rfc3339 = tomorrow.isoformat() + "Z"

    print(f"Cleaning up activities from {today} to {tomorrow}")

    # 1. Delete all sessions for today
    try:
        # Use RFC3339 format for session listing
        sessions = fitness_service.users().sessions().list(
            userId='me',
            startTime=start_time_rfc3339,
            endTime=end_time_rfc3339
        ).execute()

        if 'session' in sessions:
            for session in sessions['session']:
                session_id = session['id']
                try:
                    fitness_service.users().sessions().delete(
                        userId='me',
                        sessionId=session_id
                    ).execute()
                    print(f"Deleted session {session_id}")
                except HttpError as error:
                    print(f"Error deleting session {session_id}: {error}")
        else:
            print("No sessions found for today")
    except HttpError as error:
        print(f"Error listing sessions: {error}")

    # 2. Delete datasets only for data sources created by our app
    try:
        data_sources_result = fitness_service.users().dataSources().list(
            userId='me'
        ).execute()

        if 'dataSource' in data_sources_result:
            for data_source in data_sources_result['dataSource']:
                data_source_id = data_source['dataStreamId']

                # Only delete datasets from sources created by our script
                if PROJECT_NUMBER in data_source_id and "microcloud" in data_source_id:
                    dataset_id = f"{start_time_ns}-{end_time_ns}"

                    try:
                        fitness_service.users().dataSources().datasets().delete(
                            userId='me',
                            dataSourceId=data_source_id,
                            datasetId=dataset_id
                        ).execute()
                        print(f"Deleted dataset for {data_source_id}")
                    except HttpError as error:
                        # Ignore 404 errors (dataset doesn't exist)
                        if error.resp.status != 404:
                            print(f"Error deleting dataset for {data_source_id}: {error}")
        else:
            print("No data sources found")
    except HttpError as error:
        print(f"Error listing data sources: {error}")

    print("Cleanup completed")
    return True

if __name__ == "__main__":
    # Use command line arguments to control the script
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Log fitness activities to Google Fit')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up today\'s activities')

    # Log activity command
    log_parser = subparsers.add_parser('log', help='Log an activity')
    log_parser.add_argument('--type', type=int, default=8,
                           help='Activity type (default: 8 for running)')
    log_parser.add_argument('--duration', type=int, default=30,
                           help='Duration in minutes (default: 30)')
    log_parser.add_argument('--calories', type=float,
                           help='Calories burned (optional)')
    log_parser.add_argument('--steps', type=int,
                           help='Number of steps (optional)')
    log_parser.add_argument('--hours-ago', type=float, default=1,
                           help='How many hours ago the activity ended (default: 1)')

    args = parser.parse_args()

    if args.command == 'cleanup':
        clean_up_todays_activities()
    elif args.command == 'log':
        now = datetime.datetime.now()
        end_time = now - datetime.timedelta(hours=args.hours_ago)
        start_time = end_time - datetime.timedelta(minutes=args.duration)

        success = log_activity(args.type, start_time, end_time,
                             calories=args.calories, steps=args.steps)

        if success:
            print("Activity logged successfully!")
        else:
            print("Failed to log activity.")
    else:
        # Default behavior if no command provided
        now = datetime.datetime.now()
        end_time = now - datetime.timedelta(hours=1)
        start_time = end_time - datetime.timedelta(minutes=30)

        # 8 is the activity type for running
        success = log_activity(8, start_time, end_time, calories=250, steps=3500)

        if success:
            print("Activity logged successfully!")
        else:
            print("Failed to log activity.")