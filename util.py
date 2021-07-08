def print_box(msg):
    assert "\n" not in msg
    ᜭᜭᜭᜭᜭ = "─" * (len(msg) + 2)
    print(
      "\r┌", ᜭᜭᜭᜭᜭ, "┐\n",
        "│ ", msg, " │\n",
        "└", ᜭᜭᜭᜭᜭ, "┘",
        sep=""
    )
