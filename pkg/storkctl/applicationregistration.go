package storkctl

import (
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var applicationRegistrationColumns = []string{"NAME", "KIND", "CRD-NAME", "VERSION", "SUSPEND-OPTIONS", "KEEP-STATUS"}
var applicationRegistrationSubcommand = "applicationregistrations"
var applicationRegistrationAliases = []string{"applicationregistration", "appreg", "appregs"}

func newGetapplicationRegistrationCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getapplicationRegistrationCommand := &cobra.Command{
		Use:     applicationRegistrationSubcommand,
		Aliases: applicationRegistrationAliases,
		Short:   "Get applicationRegistration resources",
		Run: func(c *cobra.Command, args []string) {
			var appRegList *storkv1.ApplicationRegistrationList

			if len(args) > 0 {
				for _, name := range args {
					appRegList = new(storkv1.ApplicationRegistrationList)
					reg, err := storkops.Instance().GetApplicationRegistration(name)
					if err != nil {
						util.CheckErr(err)
						return
					}
					appRegList.Items = append(appRegList.Items, *reg)
				}
			} else {
				var tmpAppReg storkv1.ApplicationRegistrationList
				appRegs, err := storkops.Instance().ListApplicationRegistrations()
				if err != nil {
					util.CheckErr(err)
					return
				}
				tmpAppReg.Items = append(tmpAppReg.Items, appRegs.Items...)
				appRegList = &tmpAppReg
			}

			if len(appRegList.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			if err := printObjects(c, appRegList, cmdFactory, applicationRegistrationColumns, applicationRegistrationPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getapplicationRegistrationCommand.Flags())

	return getapplicationRegistrationCommand
}

func applicationRegistrationPrinter(
	applicationRegistrationList *storkv1.ApplicationRegistrationList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if applicationRegistrationList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, app := range applicationRegistrationList.Items {
		for _, res := range app.Resources {
			suspendOptions := ""
			if res.SuspendOptions.Path != "" {
				suspendOptions = res.SuspendOptions.Path + "," + res.SuspendOptions.Type
				if res.SuspendOptions.Value != "" {
					suspendOptions = suspendOptions + "," + res.SuspendOptions.Value
				}
			}
			row := getRow(&app,
				[]interface{}{app.Name,
					res.Kind,
					res.Group,
					res.Version,
					suspendOptions,
					res.KeepStatus},
			)
			rows = append(rows, row)
		}
	}
	return rows, nil
}
