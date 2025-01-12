import yamale

fichier_schema_yaml = '/home/mcolinde/Documents/REFERENCE_SYSTEM/rs-client-libraries/rs_client/staging_templates/test_process.yaml'
schema = yamale.make_schema(fichier_schema_yaml)

data = yamale.make_data(content="""
name: Bill
age: 26
height: 6.2
awesome: True
"""
)
print(data)
try:
    yamale.validate(schema, data)
    print('Validation success! 👍')
except ValueError as e:
    print('Validation failed!\n%s' % str(e))
    exit(1)