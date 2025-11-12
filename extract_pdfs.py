#!/usr/bin/env python3
"""
AEO Corporate Response Analysis - PDF Text Extraction from GitHub
==================================================================

This script extracts text from AEO PDFs hosted on GitHub with page-level tracking.
"""

import fitz  # PyMuPDF
import os
import json
import pandas as pd
import re
from datetime import datetime
import requests
from io import BytesIO

class AEOGitHubPDFExtractor:
    def __init__(self, github_repo="sc22112350-creator/aeo_corporate_response_analysis", 
                 branch="main", output_dir="./aeo_extracted_data"):
        """
        Initialize extractor with GitHub repository details
        
        Args:
            github_repo: GitHub username/repo format
            branch: Branch name (usually 'main' or 'master')
            output_dir: Local directory to save extracted data
        """
        self.github_repo = github_repo
        self.branch = branch
        self.base_url = f"https://raw.githubusercontent.com/{github_repo}/{branch}"
        self.output_dir = output_dir
        self.text_corpus_dir = os.path.join(self.output_dir, "text_corpus")

        # Create output directories
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.text_corpus_dir, exist_ok=True)

        self.extracted_data = []
        self.document_metadata = []

    def define_pdf_files_structure1(self):
        """
        Option 1: Files organized by year folders (like Gap structure)
        e.g., AEO_2020/file.pdf, AEO_2021/file.pdf
        """
        pdf_files = []
        
        # Define your file structure here
        file_mappings = {
            2020: ['AEO_2020_Impact_Report.pdf', 'AEO_2020_10K.pdf'],
            2021: ['AEO_2021_Impact_Report.pdf', 'AEO_2021_10K.pdf'],
            2022: ['AEO_2022_Impact_Report.pdf', 'AEO_2022_10K.pdf'],
            2023: ['AEO_2023_Impact_Report.pdf', 'AEO_2023_10K.pdf'],
            2024: ['AEO_2024_Impact_Report.pdf', 'AEO_2024_10K.pdf'],
        }
        
        for year, filenames in file_mappings.items():
            for filename in filenames:
                pdf_files.append({
                    'year': year,
                    'filename': filename,
                    'path': f'AEO_{year}/{filename}',
                    'doc_type': self.classify_document_type(filename)
                })
        
        return pdf_files

    def define_pdf_files_structure2(self):
        """
        Option 2: All files in root or single folder
        e.g., pdfs/file1.pdf, pdfs/file2.pdf
        """
        pdf_files = [
            {'year': 2020, 'filename': 'AEO_2020_Impact_Report.pdf', 'path': 'pdfs/AEO_2020_Impact_Report.pdf'},
            {'year': 2021, 'filename': 'AEO_2021_Impact_Report.pdf', 'path': 'pdfs/AEO_2021_Impact_Report.pdf'},
            # Add all your files here
        ]
        
        # Add document type classification
        for pdf in pdf_files:
            pdf['doc_type'] = self.classify_document_type(pdf['filename'])
        
        return pdf_files

    def auto_discover_pdfs(self):
        """
        Option 3: Attempt to auto-discover PDFs via GitHub API
        Note: This requires the repo to be public
        """
        try:
            api_url = f"https://api.github.com/repos/{self.github_repo}/git/trees/{self.branch}?recursive=1"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            
            tree = response.json().get('tree', [])
            pdf_files = []
            
            for item in tree:
                path = item['path']
                if path.lower().endswith('.pdf'):
                    # Extract year from path if possible
                    year_match = re.search(r'20\d{2}', path)
                    year = int(year_match.group()) if year_match else 2024
                    
                    filename = os.path.basename(path)
                    pdf_files.append({
                        'year': year,
                        'filename': filename,
                        'path': path,
                        'doc_type': self.classify_document_type(filename)
                    })
            
            return pdf_files
        
        except Exception as e:
            print(f"Auto-discovery failed: {e}")
            print("Falling back to manual file list...")
            return []

    def classify_document_type(self, filename):
        """Classify document type based on filename"""
        filename_lower = filename.lower()

        if 'impact' in filename_lower or 'sustainability' in filename_lower or 'esg' in filename_lower:
            return 'Impact Report'
        elif 'assurance' in filename_lower:
            return 'Assurance Statement'
        elif 'proxy' in filename_lower:
            return 'Proxy Statement'
        elif '10k' in filename_lower or '10-k' in filename_lower:
            return 'Form 10-K'
        elif 'carbon' in filename_lower:
            return 'Carbon Disclosure'
        else:
            return 'Other'

    def download_pdf_from_github(self, github_path):
        """Download PDF from GitHub"""
        url = f"{self.base_url}/{github_path}"
        print(f"    Fetching: {url}")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        return BytesIO(response.content)

    def extract_text_from_pdf(self, pdf_stream):
        """Extract text from PDF with page-level tracking"""
        doc = fitz.open(stream=pdf_stream, filetype="pdf")
        pages_data = []

        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            text = self.clean_text(text)

            pages_data.append({
                'page_number': page_num + 1,
                'text': text,
                'char_count': len(text),
                'word_count': len(text.split())
            })

        doc.close()

        return {
            'success': True,
            'total_pages': len(pages_data),
            'pages': pages_data
        }

    def clean_text(self, text):
        """Clean extracted text"""
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        return text.strip()

    def extract_all_documents(self, use_auto_discover=True):
        """Main extraction workflow"""
        print("=" * 80)
        print("AEO PDF Text Extraction from GitHub")
        print("=" * 80)
        print(f"\nRepository: {self.github_repo}")
        print(f"Branch: {self.branch}")
        print(f"Starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Get PDF file list
        if use_auto_discover:
            pdf_files = self.auto_discover_pdfs()
            if not pdf_files:
                print("Using manual file list (Structure 1)...")
                pdf_files = self.define_pdf_files_structure1()
        else:
            # Change this to structure2() if needed
            pdf_files = self.define_pdf_files_structure1()
        
        print(f"Found {len(pdf_files)} PDF documents to process\n")

        # Process each PDF
        for idx, pdf_info in enumerate(pdf_files, 1):
            print(f"[{idx}/{len(pdf_files)}] {pdf_info['filename']}")
            print(f"    Year: {pdf_info['year']} | Type: {pdf_info['doc_type']}")

            try:
                pdf_stream = self.download_pdf_from_github(pdf_info['path'])
                extraction_result = self.extract_text_from_pdf(pdf_stream)

                print(f"    ✓ Extracted {extraction_result['total_pages']} pages")

                self.document_metadata.append({
                    'year': pdf_info['year'],
                    'filename': pdf_info['filename'],
                    'doc_type': pdf_info['doc_type'],
                    'total_pages': extraction_result['total_pages'],
                    'github_path': pdf_info['path']
                })

                self.save_text_corpus(pdf_info, extraction_result)
                self.build_dataset_records(pdf_info, extraction_result)

            except Exception as e:
                print(f"    ✗ Error: {str(e)}")

            print()

        self.save_outputs()

        print("\n" + "=" * 80)
        print("Extraction Complete!")
        print("=" * 80)
        print(f"\nOutputs: {self.output_dir}")
        print(f"Total documents: {len(self.document_metadata)}")
        print(f"Total pages: {sum(doc['total_pages'] for doc in self.document_metadata)}")

    def save_text_corpus(self, pdf_info, extraction_result):
        """Save full text corpus"""
        safe_filename = re.sub(r'[^a-zA-Z0-9_-]', '_', 
                              pdf_info['filename'].replace('.pdf', ''))
        output_path = os.path.join(self.text_corpus_dir, 
                                   f"{pdf_info['year']}_{safe_filename}_fulltext.txt")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"{'=' * 80}\n")
            f.write(f"AEO - {pdf_info['doc_type']}\n")
            f.write(f"Year: {pdf_info['year']}\n")
            f.write(f"Filename: {pdf_info['filename']}\n")
            f.write(f"Total Pages: {extraction_result['total_pages']}\n")
            f.write(f"{'=' * 80}\n\n")

            for page_data in extraction_result['pages']:
                f.write(f"\n{'─' * 80}\n")
                f.write(f"PAGE {page_data['page_number']}\n")
                f.write(f"{'─' * 80}\n\n")
                f.write(page_data['text'])
                f.write("\n\n")

    def build_dataset_records(self, pdf_info, extraction_result):
        """Build structured dataset"""
        for page_data in extraction_result['pages']:
            self.extracted_data.append({
                'year': pdf_info['year'],
                'document_type': pdf_info['doc_type'],
                'filename': pdf_info['filename'],
                'page_number': page_data['page_number'],
                'text': page_data['text'],
                'word_count': page_data['word_count'],
                'char_count': page_data['char_count']
            })

    def save_outputs(self):
        """Save all outputs"""
        df = pd.DataFrame(self.extracted_data)
        df.to_csv(os.path.join(self.output_dir, 'aeo_master_dataset.csv'),
                  index=False, encoding='utf-8')

        with open(os.path.join(self.output_dir, 'document_metadata.json'), 'w') as f:
            json.dump(self.document_metadata, f, indent=2)

        self.create_summary_report()

    def create_summary_report(self):
        """Create summary report"""
        with open(os.path.join(self.output_dir, 'extraction_summary.txt'), 'w') as f:
            df = pd.DataFrame(self.document_metadata)
            
            f.write("AEO PDF Extraction Summary\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("Documents by Year:\n")
            f.write("-" * 40 + "\n")
            
            for year in sorted(df['year'].unique()):
                year_docs = df[df['year'] == year]
                f.write(f"\n{year}: {len(year_docs)} documents, {year_docs['total_pages'].sum()} pages\n")
                for _, doc in year_docs.iterrows():
                    f.write(f"  - {doc['doc_type']}: {doc['total_pages']} pages\n")

            f.write(f"\n{'=' * 80}\n")
            f.write(f"Total: {len(df)} documents, {df['total_pages'].sum()} pages\n")

def main():
    # Initialize with your repo
    extractor = AEOGitHubPDFExtractor(
        github_repo="sc22112350-creator/aeo_corporate_response_analysis",
        branch="main"  # Change to "master" if needed
    )
    
    # Try auto-discovery first, falls back to manual list if it fails
    extractor.extract_all_documents(use_auto_discover=True)

if __name__ == "__main__":
    main()