from src.auth.permission_enum import Actions, Modules, Service


service = list(Service)
module = list(Modules)
action = list(Actions)
permissions = []
for i in range(len(service)):
    for j in range(len(module)):
        for k in range(len(action)):
            perm = {
                "service": service[i].name,
                "serviceTitle": service[i].value,
                "module": module[j].name,
                "moduleTitle": module[j].value,
                "action": action[k].name,
                "actionTitle": action[k].value,
            }
            permissions.append(perm)
