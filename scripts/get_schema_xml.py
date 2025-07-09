# Filename: scripts/get_schema.py
"""Script to extract database schema from DuckDB and output as XML.

This script connects to a DuckDB database, extracts the schema information,
and outputs it in a machine-readable XML format that can be used in Cursor.

Source: https://github.com/matsonj/cursor_eda/blob/main/scripts/get_schema.py
"""

import duckdb
import xml.etree.ElementTree as ET
from pathlib import Path
import os

def get_schema_as_xml(db_path: str) -> ET.Element:
    """Extract schema from DuckDB database and return as XML Element.
    
    Args:
        db_path: Path to the DuckDB database file
        
    Returns:
        ET.Element: XML Element containing the database schema
    """
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # Get all tables
    tables = conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema in ('main', 'bronze_fhvhv', 'bronze_green_trip', 'bronze_yellow_trip')
        """).fetchall()
    
    # Create XML root
    root = ET.Element("database")
    root.set("name", Path(db_path).stem)
    
    # For each table, get its schema
    for (table_name,) in tables:
        table_elem = ET.SubElement(root, "table")
        table_elem.set("name", table_name)
        
        # Get column information
        columns = conn.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema in ('main', 'bronze_fhvhv', 'bronze_green_trip', 'bronze_yellow_trip') AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        for col_name, data_type, is_nullable in columns:
            column_elem = ET.SubElement(table_elem, "column")
            column_elem.set("name", col_name)
            column_elem.set("type", data_type)
            column_elem.set("nullable", is_nullable)
    
    conn.close()
    return root

def save_schema_to_file(root: ET.Element, output_path: str) -> None:
    """Save the XML schema to a file with pretty printing.
    
    Args:
        root: XML Element containing the schema
        output_path: Path where to save the XML file
    """
    ET.indent(root)
    tree = ET.ElementTree(root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

if __name__ == "__main__":
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory: {script_dir}")
    project_root = os.path.dirname(script_dir)
    print(f"Project root: {project_root}")
    
    db_path = os.getenv("DUCKDB_DATABASE") or os.path.join(project_root, "data", "nyc_database.duckdb")
    output_path = os.getenv("DUCKDB_SCHEMA_XML") or os.path.join(project_root, "data", "nyc_db_schema.xml")
    
    root = get_schema_as_xml(db_path)
    save_schema_to_file(root, output_path)
    print(f"Schema saved to {output_path}")
