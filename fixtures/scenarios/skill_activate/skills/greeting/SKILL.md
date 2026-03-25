---
name: greeting
description: "Generate personalized greeting messages. Trigger when the user asks to greet someone, write a welcome message, or create a greeting card."
---

# Greeting Skill

## Rules
- Always include the person's name in the greeting
- Use the target language specified by the user (default: English)
- MUST end with an exclamation mark

## Workflow
1. Read the person's name from the user request
2. Write the greeting to a file using file_write action
3. The greeting format is: "Hello, {name}! Welcome aboard!"
