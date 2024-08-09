import pkg_resources

def get_installed_packages():
    packages = pkg_resources.working_set
    return [f"{package.project_name} {package.version}" for package in packages]

if __name__ == "__main__":
    installed_packages = get_installed_packages()
    for package in installed_packages:
        print(package)