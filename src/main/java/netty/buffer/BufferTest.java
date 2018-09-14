package netty.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.Charset;

/**
 * @author zhaomingyuan
 * @date 18-9-14
 * @time 上午11:31
 */
public class BufferTest {

    // 读写ByteBuf
    @Test
    public void test01(){
        ByteBuf byteBuf = Unpooled.copiedBuffer("learning netty !!!", Charset.forName("utf-8"));
        System.out.println(ByteBufUtil.hexDump(byteBuf));
        System.out.println(byteBuf.readCharSequence(byteBuf.readableBytes(), Charset.defaultCharset()));
    }

    @Test
    public void test02(){

    }
}
