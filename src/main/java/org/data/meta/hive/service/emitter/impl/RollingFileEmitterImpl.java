package org.data.meta.hive.service.emitter.impl;

import java.io.IOException;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.data.meta.hive.model.event.Event;
import org.data.meta.hive.service.codec.EventCodecs;
import org.data.meta.hive.service.emitter.EventEmitter;

public class RollingFileEmitterImpl implements EventEmitter {

    private static final RollingFileWriter rollingFileWriter = new RollingFileWriter(50000, "hook.event");

    public RollingFileEmitterImpl() {
    }

    public <T> void emit(Event<T> event) throws IOException {
        String message = new String(EventCodecs.encode(event));
        //这种控制台打印有可能会打印两次，不推荐这种打印方式
        SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printInfo(message);
        }

        //如果需要将该血缘信息写到其他文件，可以开启这个方法
        rollingFileWriter.writeLineWithLock(message);
    }
}
