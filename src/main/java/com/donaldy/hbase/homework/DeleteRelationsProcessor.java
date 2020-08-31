package com.donaldy.hbase.homework;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * uid1 解除 uid2关系， uid2 同时 解除 uid1 关系
 *
 * uid1 ： currUser
 * uid2 ： otherUser
 *
 * @author donald
 * @date 2020/09/01
 */
public class DeleteRelationsProcessor extends BaseRegionObserver {

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                           Durability durability) throws IOException {

        final HTableInterface relations = e.getEnvironment().getTable(TableName.valueOf("relations"));

        // 获取 rowkey ： uid1
        String currUser = new String(delete.getRow());

        // 获取 uid1 第一个 column
        Cell cell = delete.getFamilyCellMap().get(Bytes.toBytes("friends")).get(0);

        // 创建 uid2， 并设置需要删除的 column
        Delete otherUserDelete = new Delete(cell.getQualifier());
        otherUserDelete.addColumns(Bytes.toBytes("friends"), Bytes.toBytes(currUser));

        relations.delete(delete);

        // 关闭 table 对象
        relations.close();
    }
}
