{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Store where

import Database.Persist
import qualified Database.Persist.Sql as Sql
-- import Database.Persist.Class
-- import Database.Persist.Types
import Control.Exception
import Control.Applicative
import Control.Monad.IO.Class (MonadIO (..))
import qualified Database.Zookeeper as Z
import Data.Monoid
import qualified Data.Text as T
-- import qualified Data.ByteString.Char8 as B
import Database.Persist.Zookeeper.Config (ZookeeperT(..), thisConnection)
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.ZooUtil
import Data.Pool
import Control.Monad
import Control.Monad.Reader
import qualified Data.Aeson as A

--data ZookeeperBackend

  
-- -- | Execute Zookeeper transaction inside ZookeeperT monad transformer
execZookeeperT :: (Read a,Show a,Monad m, MonadIO m) => (Z.Zookeeper -> IO (Either Z.ZKError a)) -> ReaderT ZooStat m a
execZookeeperT action = do
    s <- ask
    r <- liftIO $ zExec s action
    case r of
      (Right x) -> return x
      (Left x)  -> liftIO $ throwIO $ userError $ "Zookeeper error: code" ++ show x --fail $ show x

-- instance PersistField DB.ObjectId where
--     toPersistValue = oidToPersistValue
--     fromPersistValue oid@(PersistObjectId _) = Right $ persistObjectIdToDbOid oid
--     fromPersistValue (PersistByteString bs) = fromPersistValue (PersistObjectId bs)
--     fromPersistValue _ = Left $ T.pack "expected PersistObjectId"

-- instance Sql.PersistFieldSql T.Text where
--     sqlType _ = Sql.SqlOther "doesn't make much sense for MongoDB"

instance Sql.PersistFieldSql (BackendKey ZooStat) where
    sqlType _ = Sql.SqlOther "doesn't make much sense for MongoDB"

instance A.ToJSON (BackendKey ZooStat) where
    toJSON (ZooKey key) = A.String key

instance A.FromJSON (BackendKey ZooStat) where
    parseJSON (A.String key) = pure $ ZooKey key
    parseJSON _ = mzero


instance PersistStore ZooStat where
    --type PersistMonadBackend (ZookeeperT m) = ZookeeperBackend
    newtype BackendKey ZooStat = ZooKey { unZooKey :: T.Text }
        deriving (Show, Read, Eq, Ord, PersistField)

    insert val = do
      let dir = entity2path val
      let path = dir <> "/"
      str <- execZookeeperT $ \zk -> do
        zCreate zk dir path (Just (entity2bin val)) [Z.Sequence]
      return $ txtToKey $ T.pack $ str

    insertKey key val = do
      _ <- execZookeeperT $ \zk -> do
        let dir = entity2path val
        zCreate zk dir (T.unpack (keyToTxt key)) (Just (entity2bin val)) []
      return ()

    insertKey _ _ = fail "Wrong key type in insertKey"

    repsert key val = do
      _ <- execZookeeperT $ \zk -> do
        let dir = entity2path val
        zRepSert zk dir (T.unpack (keyToTxt key)) (Just (entity2bin val))
      return ()
    repsert _ _ = fail "Wrong key type in repsert"

    replace key val = do
      execZookeeperT $ \zk -> do
        _ <- zReplace zk (T.unpack (keyToTxt key)) (Just (entity2bin val))
        return $ Right ()
      return ()

    replace _ _ = fail "Wrong key type in replace"

    delete key = do
      execZookeeperT $ \zk -> do
        _ <- Z.delete zk (T.unpack (keyToTxt key)) Nothing
        return $ Right ()
      return ()
    delete _ = fail "Wrong key type in delete"

    get key = do
      r <- execZookeeperT $ \zk -> do
        val <- Z.get zk (T.unpack (keyToTxt key)) Nothing
        return $ Right val
      case r of
        (Left Z.NoNodeError) ->
          return Nothing
        (Left v) ->
          fail $ show v
        (Right (Just str,_sta)) -> do
          return (bin2entity str)
        (Right (Nothing,_stat)) -> do
          fail $ "data is nothing"
      
    get  _ = fail "Wrong key type in get"
    update key valList = do
      va <- get key
      case va of
        Nothing -> return ()
        Just v ->
          case updateEntity v valList of
            Right v' -> 
              replace key v'
            Left v' -> error $ show v'
