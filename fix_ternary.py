#!/usr/bin/env python3
"""Fix the broken ternary operator"""

with open('frontend/src/App.jsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Find and fix the broken ternary
old_text = """      const qs = params.toString()
      const url = qs
        : 'http://localhost:8000/reports/members_collections'"""

new_text = """      const qs = params.toString()
      const url = `http://localhost:8000/reports/members_collections?${qs}`"""

if old_text in content:
    content = content.replace(old_text, new_text)
    print("✓ Fixed broken ternary operator")
else:
    print("✗ Could not find the broken ternary pattern")
    # Try another approach - look for the pattern with more context
    if "const qs = params.toString()" in content:
        print("Found const qs line, checking context...")
        # Check what's actually there
        idx = content.find("const qs = params.toString()")
        print(content[idx:idx+200])

with open('frontend/src/App.jsx', 'w', encoding='utf-8') as f:
    f.write(content)
