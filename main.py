import requests
import csv
import time
from datetime import datetime, timedelta
from config import GITHUB_API_URL, GITHUB_TOKEN

EXCLUDED_COMPANIES = ['google', 'amazon', 'microsoft', 'facebook', 'cognizant', 'accenture', 'tcs', 'deloitte', 'kpmg', 'cloudflare', 'linkedin', 'uber']
EXCLUDED_INDUSTRIES = ['fintech', 'healthtech']
SENIORITY_KEYWORDS = ['staff engineer', 'principal engineer', 'senior manager', 'director', 'senior director', 'engineer']
STARTUP_KEYWORDS = ['advisor', 'vc', 'angel investor', 'startup']

headers = {
    'Authorization': f'token {GITHUB_TOKEN}'
}

# Load existing users from CSV
def load_existing_users(csv_file='champions.csv'):
    existing_users = set()
    try:
        with open(csv_file, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_users.add(row['name'])  # Get and add username to the set
    except FileNotFoundError:
        print(f"{csv_file} not found, creating a new one.")
    return existing_users

# Function to check rate limits
def check_rate_limit():
    response = requests.get(f"{GITHUB_API_URL}/rate_limit", headers=headers)
    if response.status_code == 200:
        data = response.json()
        search_remaining = data['resources']['search']['remaining']
        search_reset = data['resources']['search']['reset']
        return search_remaining, search_reset
    else:
        print("Failed to check rate limit.")
        return

# Function to scrape GitHub profiles with pagination
def scrape_github(existing_users, page=1):
    print(f"Scraping page {page}...")

    # Check the rate limit before making the search request
    search_remaining, search_reset = check_rate_limit()

    if search_remaining == 0:
        reset_time = datetime.fromtimestamp(search_reset)
        current_time = datetime.now()
        wait_time = (reset_time - current_time).total_seconds()
        print(f"Rate limit reached, waiting for {int(wait_time)} seconds until reset...")
        time.sleep(wait_time)  # Wait for rate limit to reset

    params = {
        'q': 'senior OR principal OR director OR manager repos:>5',
        'type': 'User',
        'page': page,
        'per_page': 100
    } # Search params
    
    

    response = requests.get(f"{GITHUB_API_URL}/search/users", headers=headers, params=params)
    
    if response.status_code == 403:
        print("403 Forbidden: Rate limit exceeded or token issue. Retrying after a short break...")
        time.sleep(60)
        return scrape_github(existing_users, page)
    
    if response.status_code == 200:
        data = response.json()
        users_count = len(data['items'])  # Number of users returned
        print(f"Page {page} returned {users_count} users.")  # Log the number of users per page

        if users_count == 0:
            print(f"No more users to process on page {page}. Stopping.")
            return False  # Stop the pagination if no users are returned

        for profile in data['items']:
            profile_data = get_github_profile_data(profile['login'])

            if profile_data:
                username = profile['login']
                if username in existing_users:
                    print(f"User {username} already exists in the CSV. Skipping.")
                    continue  # Skip users that are already in the CSV

                # print(f"Checking profile: {username}")

                company_name = profile_data.get('company', '')
                bio = profile_data.get('bio', '')

                if is_excluded_company(company_name):
                    print(f"Skipped {username} due to excluded company: {company_name}")
                    continue
                
                if is_excluded_industry(bio):
                    print(f"Skipped {username} due to excluded industry in bio.")
                    continue
                
                role = extract_senior_title(bio)
                if role:
                    profile_info = {
                        'name': username,
                        'location': profile_data.get('location'),
                        'followers': profile_data.get('followers'),
                        'repos': profile_data.get('public_repos'),
                        'url': profile_data.get('html_url'),
                        'recent_commits': get_recent_commit_count(profile_data.get('login')),
                        'role': role
                    }

                    if profile_info['recent_commits'] >= 5:
                        append_to_csv(profile_info)  # Save profile to CSV immediately
                        existing_users.add(username)  # Add the user to the set of existing users
                        print(f"User {username} added to champions list and saved to CSV.")
                    else:
                        print(f"User {username} did not meet the commit activity criteria.")
                else:
                    print(f"User {username} did not have a senior title.")
            else:
                print(f"Failed to fetch data for {profile['login']}")
    else:
        print(f"Error: {response.status_code}")
    return True  # Continue scraping

# Function to fetch profile data for each user
def get_github_profile_data(username):
    response = requests.get(f"{GITHUB_API_URL}/users/{username}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch profile for {username}, status code: {response.status_code}")
        return None

# Function to extract seniority title from bio
def extract_senior_title(bio):
    bio = bio.lower() if bio else ''
    for keyword in SENIORITY_KEYWORDS:
        if keyword in bio:
            return keyword.capitalize()
    for keyword in STARTUP_KEYWORDS:
        if keyword in bio:
            return 'Startup/VC involvement'
    return None

# Function to check if the company is excluded
def is_excluded_company(company_name):
    if not company_name:
        return False
    company_name = company_name.lower()
    return any(excluded in company_name for excluded in EXCLUDED_COMPANIES)

# Function to check if the bio indicates an excluded industry
def is_excluded_industry(bio):
    if not bio:
        return False
    bio = bio.lower()
    return any(excluded in bio for excluded in EXCLUDED_INDUSTRIES)

# Function to get recent commit count from user's activity
def get_recent_commit_count(username):
    last_month = datetime.now() - timedelta(days=30)
    response = requests.get(f"{GITHUB_API_URL}/users/{username}/events", headers=headers)
    
    if response.status_code == 200:
        events = response.json()
        commit_count = 0

        for event in events:
            if event['type'] == 'PushEvent':
                event_date = datetime.strptime(event['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                if event_date >= last_month:
                    commit_count += 1

        return commit_count
    return 0

# Append profiles to the CSV file without overwriting
def append_to_csv(profile):
    with open('champions.csv', mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['name', 'location', 'followers', 'repos', 'url', 'recent_commits', 'role'])
        if file.tell() == 0:
            writer.writeheader()  # Write header if the file is empty
        writer.writerow(profile)
        print(f"Profile {profile['name']} saved to CSV.")

# Run the scraper
if __name__ == "__main__":
    existing_users = load_existing_users()  # Load existing users into a set
    page = 1
    while True:
        should_continue = scrape_github(existing_users, page=page)
        if not should_continue:
            break  # Stop scraping if no more users are returned
        page += 1
