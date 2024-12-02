import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sqlite3

# Set up Spotipy with your credentials
sp = spotipy.Spotify(auth_manager = SpotifyClientCredentials(
    client_id = "1c4e8e0054e54852935f7e18802850a8",
    client_secret = "89384f70d719433fa44a6fc0fd2c4cbd"
))

# Connect to SQLite database (this will create the file if it doesn't exist)
conn = sqlite3.connect('spotify_data.db')
c = conn.cursor()

# Drop existing tables if needed (for testing purposes)
c.execute('DROP TABLE IF EXISTS artists')
c.execute('DROP TABLE IF EXISTS songs')

# Create artists table
c.execute('''
CREATE TABLE IF NOT EXISTS artists (
    artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_name TEXT UNIQUE,
    monthly_listeners INTEGER
)
''')

# Create songs table
c.execute('''
CREATE TABLE IF NOT EXISTS songs (
    song_id INTEGER PRIMARY KEY AUTOINCREMENT,
    song_name TEXT UNIQUE,
    artist_id INTEGER,
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
)
''')

# Commit the schema changes
conn.commit()

# Function to insert artist data
def insert_artist(artist_name, monthly_listeners):
    try:
        c.execute('''
        INSERT OR IGNORE INTO artists (artist_name, monthly_listeners)
        VALUES (?, ?)
        ''', (artist_name, monthly_listeners))
        conn.commit()
        print(f"Inserted artist: {artist_name} with {monthly_listeners} listeners.")
    except sqlite3.IntegrityError as e:
        print(f"Error inserting artist: {artist_name}. {e}")

# Function to insert song data
def insert_song(song_name, artist_id):
    try:
        c.execute('''
        INSERT OR IGNORE INTO songs (song_name, artist_id)
        VALUES (?, ?)
        ''', (song_name, artist_id))
        conn.commit()
        print(f"Inserted song: {song_name} for artist ID {artist_id}.")
    except sqlite3.IntegrityError as e:
        print(f"Error inserting song: {song_name}. {e}")

# Fetch and store data from Spotify API
offset = 0
while True:
    # Check counts
    c.execute('SELECT COUNT(*) FROM artists')
    artist_count = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM songs')
    song_count = c.fetchone()[0]

    # Stop if at least 100 rows are stored
    if artist_count >= 100 and song_count >= 100:
        print("Required rows reached. Stopping data collection.")
        break

    # Fetch artists (limit 25 items per run)
    results = sp.search(q='artist', type='artist', limit=25, offset=offset)
    artists = results['artists']['items']

    if not artists:
        print("No more artists found.")
        break

    for artist in artists:
        if artist_count >= 100:
            break

        artist_name = artist['name']
        monthly_listeners = artist.get('followers', {}).get('total', 0)

        # Insert artist
        insert_artist(artist_name, monthly_listeners)

        # Get artist_id
        c.execute('SELECT artist_id FROM artists WHERE artist_name = ?', (artist_name,))
        artist_id = c.fetchone()[0]

        # Fetch top tracks
        top_tracks = sp.artist_top_tracks(artist['id'])
        for track in top_tracks['tracks']:
            if song_count >= 100:
                break
            song_name = track['name']
            insert_song(song_name, artist_id)
            song_count += 1

        artist_count += 1

    offset += 25

# Close the database connection
conn.commit()
conn.close()

