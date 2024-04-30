{{ codegen.generate_source(
    schema_name= 'stage', 
    table_names= ["kla_ecc_po_ekkn"],
    generate_columns = True,
    include_data_types = False,
    include_descriptions= True,
    name = 'stage',
    include_schema =True
    
    )
    
}}