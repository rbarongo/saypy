#!/usr/bin/env python3
"""Remove misplaced label mapping code"""

with open('frontend/src/App.jsx', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and remove the misplaced mapping lines
new_lines = []
skip_until_closing_brace = False
removed_count = 0

for i, line in enumerate(lines):
    # Check if this is a misplaced mapping line (after catch block)
    stripped = line.strip()
    if (skip_until_closing_brace or 
        ("OFFICIAL_MEMBER_ID: 'Unique Member ID'" in line or
         "GROUP_NAME: 'Group Name'" in line)):
        
        # Skip these misplaced mapping lines and the extra closing brace
        if stripped == '}':
            skip_until_closing_brace = False
            removed_count += 1
            print(f"Skipped misplaced lines, removed {removed_count} total")
            continue
        else:
            skip_until_closing_brace = True
            removed_count += 1
            print(f"Removing misplaced line {i}: {line.strip()[:50]}")
            continue
    
    new_lines.append(line)

# Write back
with open('frontend/src/App.jsx', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"✓ Removed {removed_count} misplaced mapping lines")
