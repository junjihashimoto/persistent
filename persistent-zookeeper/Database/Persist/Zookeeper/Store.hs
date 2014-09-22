{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Store 
    ( ZookeeperBackend
    , execZookeeperT
    )where

import Database.Persist
-- import Database.Persist.Class
-- import Database.Persist.Types
import Control.Exception
import Control.Applicative
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import qualified Database.Zookeeper as Z
import Data.Monoid
import qualified Data.Text as T
-- import qualified Data.ByteString.Char8 as B
import Database.Persist.Zookeeper.Config (ZookeeperT(..), thisConnection)
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.ZooUtil
import Data.Pool


data ZookeeperBackend

  
-- -- | Execute Zookeeper transaction inside ZookeeperT monad transformer
execZookeeperT :: (Read a,Show a,Monad m, MonadIO m) => (Z.Zookeeper -> IO (Either Z.ZKError a)) -> ZookeeperT m a
execZookeeperT action = do
    conn <- thisConnection
    r <- liftIO $ withResource conn $ \s ->
      zExec s action
    case r of
      (Right x) -> return x
      (Left x)  -> liftIO $ throwIO $ userError $ "Zookeeper error: code" ++ show x --fail $ show x

instance (Applicative m, Functor m, MonadIO m, MonadBaseControl IO m) => PersistStore (ZookeeperT m) where
    type PersistMonadBackend (ZookeeperT m) = ZookeeperBackend

    insert val = do
      let dir = entity2path val
      let path = dir <> "/"
      str <- execZookeeperT $ \zk -> do
        zCreate zk dir path (Just (entity2bin val)) [Z.Sequence]
      return $ txtToKey $ T.pack $ str

    insertKey (Key (PersistText key)) val = do
      _ <- execZookeeperT $ \zk -> do
        let dir = entity2path val
        zCreate zk dir (T.unpack key) (Just (entity2bin val)) []
      return ()

    insertKey _ _ = fail "Wrong key type in insertKey"

    repsert (Key (PersistText key)) val = do
      _ <- execZookeeperT $ \zk -> do
        let dir = entity2path val
        zRepSert zk dir (T.unpack key) (Just (entity2bin val))
      return ()
    repsert _ _ = fail "Wrong key type in repsert"

    replace (Key (PersistText key)) val = do
      execZookeeperT $ \zk -> do
        _ <- zReplace zk (T.unpack key) (Just (entity2bin val))
        return $ Right ()
      return ()

    replace _ _ = fail "Wrong key type in replace"

    delete (Key (PersistText key)) = do
      execZookeeperT $ \zk -> do
        _ <- Z.delete zk (T.unpack key) Nothing
        return $ Right ()
      return ()
    delete _ = fail "Wrong key type in delete"

    get (Key (PersistText key)) = do
      r <- execZookeeperT $ \zk -> do
        val <- Z.get zk (T.unpack key) Nothing
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
