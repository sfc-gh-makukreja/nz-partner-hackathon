#!/bin/bash
set -e

echo "🎣 NZ Partner Hackathon - Fishing Documents Cortex RAG Setup"
echo "=============================================================="
echo ""
echo "This script will:"
echo "1. Process all PDF fishing regulations in data/fish-pdf/"
echo "2. Create intelligent text chunks using Cortex functions"  
echo "3. Build production-ready Cortex Search Service"
echo "4. Enable RAG applications and intelligent Q&A"
echo ""

# Check if PDF files exist
if [ ! -d "data/fish-pdf" ] || [ -z "$(ls -A data/fish-pdf 2>/dev/null)" ]; then
    echo "⚠️  No PDF files found in data/fish-pdf/"
    echo "📁 Please add fishing regulation PDFs to data/fish-pdf/ before running this script"
    echo ""
    echo "Example PDFs to download:"
    echo "- Regional fishing regulations from Fisheries NZ"
    echo "- Marine protected area guidelines"
    echo "- Commercial fishing rules"
    echo "- Recreational fishing limits"
    echo ""
    exit 1
fi

echo "📁 Found PDF files:"
ls -la data/fish-pdf/*.pdf 2>/dev/null || echo "   (No .pdf files found)"
echo ""

echo "🚀 Starting Cortex Search setup..."
echo "This will take a few minutes to process all documents..."
echo ""

# Execute the comprehensive SQL setup
snow sql --connection admin --filename scripts/setup_cortex_fishing_documents.sql

echo ""
echo "✅ Cortex Search Service Setup Complete!"
echo ""
echo "🎯 Your fishing regulations are now AI-ready for:"
echo "   • Semantic search and Q&A"
echo "   • RAG (Retrieval Augmented Generation) applications"
echo "   • Intelligent chatbots"
echo "   • Compliance checking"
echo ""
echo "🔍 Test your search service:"
echo "   snow sql --connection admin -q \"USE DATABASE nz_partner_hackathon; USE SCHEMA WAITA; SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('fishing_regulations_search', '{\\\"query\\\": \\\"snapper bag limits\\\", \\\"limit\\\": 2}');\""
echo ""
echo "📚 Build RAG applications using:"
echo "   • Service: fishing_regulations_search"
echo "   • Model: snowflake-arctic-embed-l-v2.0"
echo "   • API: SNOWFLAKE.CORTEX.SEARCH_PREVIEW()"
echo ""
echo "🎣 Ready for hackathon participants to build intelligent fishing apps!"