package com.sequoiadb.test.cl;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.exception.SDBError;
import com.sequoiadb.test.common.Constants;
import com.sequoiadb.test.common.ConstantsInsert;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.*;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestIndex {
    private static Sequoiadb sdb;
    private static CollectionSpace cs;
    private static DBCollection cl;
    private String idxName = "haha";

    @BeforeClass
    public static void setConnBeforeClass() throws Exception {

    }

    @AfterClass
    public static void DropConnAfterClass() throws Exception {

    }

    @Before
    public void setUp() throws Exception {
        // sdb
        sdb = new Sequoiadb(Constants.COOR_NODE_CONN, "", "");
        // cs
        if (sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)) {
            sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
        }

        cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
        BSONObject conf = new BasicBSONObject();
        conf.put("ReplSize", 0);
        cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
        List<BSONObject> list = ConstantsInsert.createRecordList(100);
        cl.bulkInsert(list, DBCollection.FLG_INSERT_CONTONDUP);
    }

    @After
    public void tearDown() throws Exception {
        sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
        sdb.disconnect();
    }

    @Test
    public void testCreateIndex() {
        BasicBSONObject key = new BasicBSONObject("a", 1);
        cl.createIndex(idxName, key, false, false);

        DBCursor cursor = cl.getIndex(idxName);
        assertTrue(cursor.hasNext());
    }

    @Test
    public void testGetEmptyIndex() {

        String emptyIndexName = "aaaaaaaaa";
        // case 1:
        DBCursor cursor;
        cursor = cl.getIndex(emptyIndexName);
        while(cursor.hasNext()) {
            System.out.println("index is: " + cursor.getNext());
        }

        // case 2:
        Assert.assertFalse(cl.isIndexExist(emptyIndexName));
        try {
            cl.getIndexInfo(emptyIndexName);
            Assert.fail();
        } catch (BaseException e) {
            Assert.assertEquals(SDBError.SDB_IXM_NOTEXIST.getErrorCode(),
                    e.getErrorCode());
        }

        // case 3:
        String idIdxName = "$id";
        Assert.assertTrue(cl.isIndexExist(idIdxName));
        BSONObject indexObj = cl.getIndexInfo(idIdxName);
        Assert.assertNotNull(indexObj);
        System.out.println("id index is: " + indexObj.toString());

    }
}