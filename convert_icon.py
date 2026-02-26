from PIL import Image

# change path if needed
png_path = "frontend/logo.png"
ico_path = "frontend/logo.ico"

img = Image.open(png_path)
img.save(ico_path, format="ICO", sizes=[(256, 256)])

print("ICO file created:", ico_path)
