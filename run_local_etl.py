import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
from spotify_tools import get_data, get_artists_from_playlist

playlist_URI = "spotify:playlist:37i9dQZF1DX4DZAVUAwHMT"

get_data(playlist_URI)
