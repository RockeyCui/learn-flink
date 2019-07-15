package com.rock.gendata;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ArgsOption {
    private static Options options = new Options();

    static {
        Option name = Option.builder().longOpt("name").required(true).hasArg().desc("set the out file name").build();
        Option path = Option.builder().longOpt("path").required(false).hasArg().desc("set the out file path, default user.dir").build();
        Option phone = Option.builder().longOpt("phone").required(true).hasArg().desc("set the number of mobile phone numbers send per second in the file").build();
        Option times = Option.builder().longOpt("times").required(true).hasArg().desc("set the number of mobile phone numbers send times (S) in the file").build();
        Option help = Option.builder().longOpt("help").required(false).hasArg(false).desc("list the args you need input").build();
        options.addOption(name);
        options.addOption(path);
        options.addOption(phone);
        options.addOption(times);
        options.addOption(help);
    }

    public static Options getOption() {
        return options;
    }
}
