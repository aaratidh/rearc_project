import aws_cdk as cdk
from rearc_pipeline.stack import RearcPipelineStack

app = cdk.App()
RearcPipelineStack(app, "RearcPipelineStack")
app.synth()