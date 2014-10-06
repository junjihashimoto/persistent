{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes, TypeFamilies, GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Config
    (
     ZookeeperConf (..)
    , Connection
    , ZookeeperT (..)
    , runZookeeperPool
    , runZookeeper
    , withZookeeperConn
    , thisConnection
    , module Database.Persist
    ) where

import Database.Persist
import qualified Database.Persist.Zookeeper.ZooUtil as Z
import qualified Database.Zookeeper as Z
import Data.Pool
import Data.Aeson
import Control.Monad (mzero, MonadPlus(..))
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Class (MonadTrans (..))
import Control.Monad.Trans.Control
import Control.Applicative (Applicative (..))
import Control.Monad.Reader(ReaderT(..))
import Control.Monad.Reader.Class
import Data.Scientific() -- we require only RealFrac instance of Scientific
import           Data.Time

-- | Information required to connect to a Zookeeper server
data ZookeeperConf = ZookeeperConf {
    zCoord    :: String
  , zTimeout  :: Z.Timeout
  , zNumStripes :: Int
  , zIdleTime :: NominalDiffTime
  , zMaxResources :: Int
} deriving (Show)

type Connection = Pool Z.ZooStat

-- | Monad reader transformer keeping Zookeeper connection through out the work
newtype ZookeeperT m a = ZookeeperT { runZookeeperT :: ReaderT Connection m a }
    deriving (Monad, MonadIO, MonadTrans, Functor, Applicative, MonadPlus)

-- | Extracts connection from ZookeeperT monad transformer
thisConnection :: Monad m => ZookeeperT m Connection
thisConnection = ZookeeperT ask

-- | Run a connection reader function against a Zookeeper configuration
withZookeeperConn :: (Monad m, MonadIO m) => ZookeeperConf -> (Connection -> m a) -> m a
withZookeeperConn conf connectionReader = do
    conn <- liftIO $ createPoolConfig conf
    connectionReader conn

runZookeeperPool :: ZookeeperT m a -> Connection -> m a
runZookeeperPool (ZookeeperT r) = runReaderT r

--runZookeeper :: ZookeeperT m a -> Connection -> m a
runZookeeper :: MonadBaseControl IO m =>
                Connection -> ReaderT Z.ZooStat m b -> m b
runZookeeper pool action = withResource pool (\stat -> runReaderT action stat)



instance PersistConfig ZookeeperConf where
    type PersistConfigBackend ZookeeperConf = ZookeeperT
    type PersistConfigPool ZookeeperConf = Connection

    loadConfig (Object o) = do
        coord <- o .:? "coord" .!= "localhost:2181/"
        timeout <- o .:? "timeout" .!= 10000
        numstripes <- o .:? "num-stripes" .!= 1
        (idletime :: Int) <- o .:? "idletime" .!= 30
        maxresources <- o .:? "max-resource" .!= 50

        return ZookeeperConf {
            zCoord = coord
          , zTimeout = timeout
          , zNumStripes = numstripes
          , zIdleTime = fromIntegral idletime
          , zMaxResources = maxresources
        }

    loadConfig _ = mzero

    createPoolConfig (ZookeeperConf h t s idle maxres ) = 
        Z.connect h t Nothing Nothing s idle maxres

    runPool _ = runZookeeperPool
