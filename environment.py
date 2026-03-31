def before_all(context):
    """Set up test configuration from command line parameters"""
    # Store userdata for access in steps
    context.userdata = context.config.userdata